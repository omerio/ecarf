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
import io.ecarf.core.cloud.task.impl.reason.Term;
import io.ecarf.core.reason.rulebased.GenericRule;
import io.ecarf.core.reason.rulebased.Rule;
import io.ecarf.core.term.TermUtils;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Config;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.repackaged.com.google.common.base.Joiner;

/**
 * Stream the data directly into Big data service rather than through file upload
 * @author Omer Dawelbeit (omerio)
 * @deprecated
 */
public class DoReasonTask1 extends CommonTask {
	
	private final static Log log = LogFactory.getLog(DoReasonTask1.class);
	
	private static final int MAX_CACHE = 1000000;
	
	/**
	 * 
	 * @param metadata
	 * @param cloud
	 */
	public DoReasonTask1(VMMetaData metadata, CloudService cloud) {
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
		
		
		// get all the triples we care about
		Map<Term, Set<Triple>> schemaTerms = this.getAssignedSchemaTerms(terms, schemaFile, bucket);
		
		String decoratedTable = table;
		int emptyRetries = 0;
		int totalInferredTriples = 0;
		int maxRetries = Config.getIntegerProperty(Constants.REASON_RETRY_KEY, 6);
		
		// timestamp loop
		do {

			// run the reasoning queries on the big data
			this.runBigDataReasoningQueries(schemaTerms, decoratedTable);
			
			long start = System.currentTimeMillis();	
			// the schema terms that generated inferred triples
			List<String> productiveTerms = new ArrayList<>();
			
			int interimInferredTriples = 0;

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
					rows = this.cloud.saveBigQueryResultsToFile(term.getJobId(), term.getFilename()).getTotalRows();
					
				} catch(IOException ioe) {
					// transient backend errors
					log.error("failed to save query results to file, jobId: " + term.getJobId());
				}

				log.info("Query found " + rows + ", rows");

				// only process if triples are found matching this term
				if(!BigInteger.ZERO.equals(rows)) {

					int inferredTriplesCount = this.streamInferredTriplesIntoBigData(term, select, schemaTriples, rows, table);

					productiveTerms.add(term.getTerm());//inferredTriplesFile);
					
					interimInferredTriples += inferredTriplesCount;

				}
			}
			
			totalInferredTriples += interimInferredTriples;

			if(!productiveTerms.isEmpty()) {
				log.info("Successfully streamed " + interimInferredTriples + 
						", inferred triples into Big Data table for " + productiveTerms.size() + " productive terms.");
				// need to replace this load with live streaming into Bigquery
				//List<String> jobIds = this.cloud.loadLocalFilesIntoBigData(inferredFiles, table, false);
				//log.info("All inferred triples are inserted into Big Data table, completed jobIds: " + jobIds);
				
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
	 * Get the schema terms and their triples that are assigned to this node
	 * @param terms
	 * @param schemaFile
	 * @param bucket
	 * @return
	 * @throws IOException
	 */
	private Map<Term, Set<Triple>> getAssignedSchemaTerms(Set<String> terms, String schemaFile, String bucket) throws IOException {
		
		// the terms will be null if they are too large to be set as meta data
		// in this case they will be saved to a file and the filename is provided
		// as meta data
		if(terms == null) {
			// too large, probably saved as a file
			String termsFile = metadata.getValue(VMMetaData.ECARF_TERMS_FILE);
			log.info("Using json file for terms: " + termsFile);
			
			String localTermsFile = Utils.TEMP_FOLDER + termsFile;
			this.cloud.downloadObjectFromCloudStorage(termsFile, localTermsFile, bucket);

			// convert from JSON
			terms = Utils.jsonFileToSet(localTermsFile);
			
		}
		
		// download the schema triples file locally
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
		
		return schemaTerms;
	}
	
	/**
	 * Using the schema terms assigned to this node run the big data queries that are needed for reasoning.
	 * The query will run asynchronously 
	 * @param schemaTerms
	 * @param decoratedTable
	 * @throws IOException
	 */
	private void runBigDataReasoningQueries(Map<Term, Set<Triple>> schemaTerms, String decoratedTable) throws IOException {
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
	}

	/**
	 * Stream inferred triples into big data, returns the number of streamed triples
	 * @param term
	 * @param select
	 * @param schemaTriples
	 * @param rows
	 * @return
	 */
	private int streamInferredTriplesIntoBigData(Term term, List<String> select, 
			Set<Triple> schemaTriples, BigInteger rows, String table) throws IOException {

		int inferredTriplesCount = 0;
		int failedTriplesCount = 0;

		Set<Triple> inferredTriples = new HashSet<>();
		//String inferredTriplesFile =  Utils.TEMP_FOLDER + term.getEncodedTerm() + Constants.DOT_INF;

		// loop through the instance triples probably stored in a file and generate all the triples matching the schema triples set
		try (BufferedReader reader = new BufferedReader(new FileReader(term.getFilename()))) {
			//PrintWriter writer = new PrintWriter(new GZIPOutputStream(new FileOutputStream(inferredTriplesFile)))) {

			Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);

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
							//writer.println(inferredTriple.toCsv());
							inferredTriples.add(inferredTriple);
							inferredTriplesCount++;
						}

						// this is just to avoid any memory issues
						if(inferredAlready.size() > MAX_CACHE) {
							inferredAlready.clear();
							log.info("Cleared cache of inferred terms");
						}
					}

				}
			} catch(Exception e) {
				log.error("Failed to parse selected terms", e);
				failedTriplesCount++;
			}
		}

		/*productiveTerms.add(term.getTerm());//inferredTriplesFile);*/
		log.info("\nSelect Triples: " + rows + ", Inferred: " + inferredTriplesCount + 
				", Triples for term: " + term + ", Failed Triples: " + failedTriplesCount);
		
		log.info("Streaming " + inferredTriples.size() + " Unqiue Inferred Triples into big data");

		this.cloud.streamTriplesIntoBigData(inferredTriples, table);
		
		return inferredTriplesCount;


	}
	
}
