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

import com.google.api.client.repackaged.com.google.common.base.Joiner;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTask extends CommonTask {
	
	/**
	 * 
	 * @param metadata
	 * @param cloud
	 */
	public DoReasonTask(VMMetaData metadata, CloudService cloud) {
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

			List<String> inferredFiles = new ArrayList<>();

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

					int inferredTriples = 0;
					int failedTriples = 0;

					String inferredTriplesFile =  Utils.TEMP_FOLDER + term.getEncodedTerm() + Constants.DOT_INF;

					// loop through the instance triples probably stored in a file and generate all the triples matching the schema triples set
					try (BufferedReader r = new BufferedReader(new FileReader(term.getFilename()));
							PrintWriter writer = new PrintWriter(new GZIPOutputStream(new FileOutputStream(inferredTriplesFile)))) {

						Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(r);
						
						try {
							
							for (CSVRecord record : records) {

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
							}
						} catch(Exception e) {
							log.log(Level.SEVERE, "Failed to parse selected terms", e);
							failedTriples++;
						}
					}

					inferredFiles.add(inferredTriplesFile);
					log.info("\nSelect Triples: " + rows + ", Inferred: " + inferredTriples + 
							", Triples for term: " + term + ", Failed Triples: " + failedTriples);
					
					totalInferredTriples += inferredTriples;

				}
			}

			if(!inferredFiles.isEmpty()) {
				log.info("Inserting " + inferredFiles.size() + ", files into Big Data table");
				List<String> jobIds = this.cloud.loadLocalFilesIntoBigData(inferredFiles, table, false);
				log.info("All inferred triples are inserted into Big Data table, completed jobIds: " + jobIds);
				
				// reset empty retries
				emptyRetries = 0;

			} else {
				log.info("No new inferred triples");
				// increment empty retries
				emptyRetries++;
			}

			log.info("Total inferred triples so far = " + totalInferredTriples);
			
			Utils.block(Config.getIntegerProperty(Constants.REASON_SLEEP_KEY, 20));
			
			// FIXME move into the particular cloud implementation service
			long elapsed = System.currentTimeMillis() - start;
			decoratedTable =  "[" + table + "@-" + elapsed + "-]";
			
			log.info("Using table decorator: " + decoratedTable + ". Empty retries count: " + emptyRetries);

		} while(!(emptyRetries == maxRetries)); // end timestamp loop
		
		log.info("Finished reasoning, total inferred triples = " + totalInferredTriples);
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
