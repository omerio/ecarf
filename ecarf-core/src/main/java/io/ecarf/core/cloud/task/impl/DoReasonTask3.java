/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2014, Ecarf.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.ecarf.core.cloud.task.impl;

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.task.CommonTask;
import io.ecarf.core.reason.rulebased.GenericRule;
import io.ecarf.core.reason.rulebased.Rule;
import io.ecarf.core.term.TermUtils;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Config;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.StringUtils;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Reason task that saves all the inferred triples in each round in a single file then uploads it to Big data
 * rather than using individual files for each term. Hybrid big data streaming for inferred triples of 100,000 or smaller
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTask3 extends CommonTask {
	
	private static final int MAX_CACHE = 1000000;
	
	/**
	 * 
	 * @param metadata
	 * @param cloud
	 */
	public DoReasonTask3(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.Task#run()
	 */
	@Override
	public void run() throws IOException {

		String table = metadata.getValue(VMMetaData.ECARF_TABLE);
		Set<String> terms = metadata.getTerms();
		String schemaFile = metadata.getValue(VMMetaData.ECARF_SCHEMA);
		String bucket = metadata.getBucket();
		
		if(terms == null) {
			// too large, probably saved as a file
			String termsFile = metadata.getValue(VMMetaData.ECARF_TERMS_FILE);
			log.info("Using json file for terms: " + termsFile);
			
			String localTermsFile = Utils.TEMP_FOLDER + termsFile;
			this.cloud.downloadObjectFromCloudStorage(termsFile, localTermsFile, bucket);

			// convert from JSON
			terms = Utils.jsonFileToSet(localTermsFile);
			
		}
		
		String localSchemaFile = Utils.TEMP_FOLDER + schemaFile;
		// download the file from the cloud storage
		this.cloud.downloadObjectFromCloudStorage(schemaFile, localSchemaFile, bucket);

		// uncompress if compressed
		if(GzipUtils.isCompressedFilename(schemaFile)) {
			localSchemaFile = GzipUtils.getUncompressedFilename(localSchemaFile);
		}

		Map<String, Set<Triple>> allSchemaTriples = 
				TripleUtils.getRelevantSchemaTriples(localSchemaFile, TermUtils.RDFS_TBOX);

		// get all the triples we care about
		Map<Term, Set<Triple>> schemaTerms = new HashMap<>();

		for(String term: terms) {
			if(allSchemaTriples.containsKey(term)) {
				schemaTerms.put(new Term(term), allSchemaTriples.get(term));
			}
		}
		
		String decoratedTable = table;
		int emptyRetries = 0;
		int totalInferredTriples = 0;
		int maxRetries = Config.getIntegerProperty(Constants.REASON_RETRY_KEY, 6);
		
		// timestamp loop
		do {

			//List<String> inferredFiles = new ArrayList<>();

			// First of all run all the queries asynchronously and remember the jobId and filename for each term
			for(Entry<Term, Set<Triple>> entry: schemaTerms.entrySet()) {

				Term term = entry.getKey();

				// add table decoration to table name
				String query = GenericRule.getQuery(entry.getValue(), decoratedTable);	

				log.info("\nQuery: " + query);

				String jobId = this.cloud.startBigDataQuery(query);
				String encodedTerm = Utils.encodeFilename(term.getTerm());
				String filename = Utils.TEMP_FOLDER + encodedTerm + Constants.DOT_TERMS;

				// remember the filename and the jobId for this query
				term.setFilename(filename).setJobId(jobId).setEncodedTerm(encodedTerm);

			}

			long start = System.currentTimeMillis();
			
			String inferredTriplesFile =  Utils.TEMP_FOLDER + start + Constants.DOT_INF;
			
			List<String> productiveTerms = new ArrayList<>();
			
			int interimInferredTriples = 0;
			
			try(PrintWriter writer = 
					new PrintWriter(new GZIPOutputStream(new FileOutputStream(inferredTriplesFile), Constants.GZIP_BUF_SIZE))) {

				// now loop through the queries
				for(Entry<Term, Set<Triple>> entry: schemaTerms.entrySet()) {

					Term term = entry.getKey();
					log.info("Reasoning for Term: " + term);

					Set<Triple> schemaTriples = entry.getValue();
					log.info("Schema Triples: " + Joiner.on('\n').join(schemaTriples));

					List<String> select = GenericRule.getSelect(schemaTriples);

					// block and wait for each job to complete then save results to a file
					BigInteger rows = BigInteger.ZERO;

					try {
						rows = this.cloud.saveBigQueryResultsToFile(term.getJobId(), term.getFilename());

					} catch(IOException ioe) {
						// transient backend errors
						log.log(Level.WARNING, "failed to save query results to file, jobId: " + term.getJobId());
					}

					log.info("Query found " + rows + ", rows");

					// only process if triples are found matching this term
					if(!BigInteger.ZERO.equals(rows)) {
						
						int inferredTriplesCount = this.inferAndSaveTriplesToFile(term, select, schemaTriples, rows, decoratedTable, writer);
						
						productiveTerms.add(term.getTerm());

						interimInferredTriples += inferredTriplesCount;

					}
				}
			}
			
			totalInferredTriples += interimInferredTriples;

			if(interimInferredTriples > 0) {
				
				//TODO stream smaller numbers of inferred triples
				//TODO try uploading from cloud storage
				int streamingThreshold = Config.getIntegerProperty("ecarf.io.reasoning.streaming.threshold", 100000);
				
				log.info("Inserting " + interimInferredTriples + 
						", inferred triples into Big Data table for " + productiveTerms.size() + " productive terms. Filename: " + inferredTriplesFile);
				
				if(interimInferredTriples <= streamingThreshold) {
					// stream the data
					
					Set<Triple> inferredTriples = TripleUtils.loadCompressedCSVTriples(inferredTriplesFile);
					log.info("Total triples to stream into Big Data: " + inferredTriples.size());
					this.cloud.streamTriplesIntoBigData(inferredTriples, table);
					
					log.info("All inferred triples are streamed into Big Data table");
					
				} else {
					
					// directly upload the data
					List<String> jobIds = this.cloud.loadLocalFilesIntoBigData(Lists.newArrayList(inferredTriplesFile), table, false);
					log.info("All inferred triples are directly loaded into Big Data table, completed jobIds: " + jobIds);
				}
				
				// reset empty retries
				emptyRetries = 0;

			} else {
				log.info("No new inferred triples");
				// increment empty retries
				emptyRetries++;
			}

			log.info("Total inferred triples so far = " + totalInferredTriples + ", current retry count: " + emptyRetries);
			
			Utils.block(Config.getIntegerProperty(Constants.REASON_SLEEP_KEY, 20));
			
			// FIXME move into the particular cloud implementation service
			long elapsed = System.currentTimeMillis() - start;
			decoratedTable =  "[" + table + "@-" + elapsed + "-]";
			
			log.info("Using table decorator: " + decoratedTable + ". Empty retries count: " + emptyRetries);

		} while(!(emptyRetries == maxRetries)); // end timestamp loop
		
		log.info("Finished reasoning, total inferred triples = " + totalInferredTriples);
	}
	
	/**
	 * 
	 * @param term
	 * @param select
	 * @param schemaTriples
	 * @param rows
	 * @param table
	 * @param writer
	 * @return
	 * @throws IOException
	 */
	private int inferAndSaveTriplesToFile(Term term, List<String> select, 
			Set<Triple> schemaTriples, BigInteger rows, String table, PrintWriter writer) throws IOException {
		
		int inferredTriples = 0;
		int failedTriples = 0;

		// loop through the instance triples probably stored in a file and generate all the triples matching the schema triples set
		try (BufferedReader r = new BufferedReader(new FileReader(term.getFilename()), Constants.GZIP_BUF_SIZE)) {

			Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(r);

			// records will contain lots of duplicates
			Set<String> inferredAlready = new HashSet<String>();

			try {

				for (CSVRecord record : records) {

					String values = ((select.size() == 1) ? record.get(0): StringUtils.join(record.values(), ','));

					if(!inferredAlready.contains(values)) {
						inferredAlready.add(values);

						Triple instanceTriple = new Triple();

						if(select.size() == 1) {
							instanceTriple.set(select.get(0), record.get(0));
						} else {

							instanceTriple.set(select, record.values());
						}

						for(Triple schemaTriple: schemaTriples) {
							Rule rule = GenericRule.getRule(schemaTriple);
							Triple inferredTriple = rule.head(schemaTriple, instanceTriple);
							writer.println(inferredTriple.toCsv());
							inferredTriples++;
						}

						// this is just to avoid any memory issues
						if(inferredAlready.size() > MAX_CACHE) {
							inferredAlready.clear();
							log.info("Cleared cache of inferred terms");
						}
					}

				}
			} catch(Exception e) {
				log.log(Level.SEVERE, "Failed to parse selected terms", e);
				failedTriples++;
			}
		}

		//inferredFiles.add(inferredTriplesFile);
		log.info("\nSelect Triples: " + rows + ", Inferred: " + inferredTriples + 
				", Triples for term: " + term + ", Failed Triples: " + failedTriples);
		
		return inferredTriples;
	}

	/**
	 * Term details used during the querying and reasoning process
	 * @author Omer Dawelbeit (omerio)
	 *
	 */
	public class Term {
		
		private String term;
		
		private String filename;
		
		private String jobId;
		
		private String encodedTerm;

		/**
		 * @param term
		 */
		public Term(String term) {
			super();
			this.term = term;
		}

		/**
		 * @return the term
		 */
		public String getTerm() {
			return term;
		}

		/**
		 * @param term the term to set
		 */
		public Term setTerm(String term) {
			this.term = term;
			return this;
		}

		/**
		 * @return the filename
		 */
		public String getFilename() {
			return filename;
		}

		/**
		 * @param filename the filename to set
		 */
		public Term setFilename(String filename) {
			this.filename = filename;
			return this;
		}

		/**
		 * @return the jobId
		 */
		public String getJobId() {
			return jobId;
		}

		/**
		 * @param jobId the jobId to set
		 */
		public Term setJobId(String jobId) {
			this.jobId = jobId;
			return this;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this);
		}

		/**
		 * @return the encodedTerm
		 */
		public String getEncodedTerm() {
			return encodedTerm;
		}

		/**
		 * @param encodedTerm the encodedTerm to set
		 */
		public void setEncodedTerm(String encodedTerm) {
			this.encodedTerm = encodedTerm;
		}
		
		
		
	}

}
