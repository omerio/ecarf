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
package io.ecarf.core.cloud.task.processor.reason.phase2;

import io.cloudex.cloud.impl.google.GoogleCloudService;
import io.cloudex.framework.cloud.api.ApiUtils;
import io.cloudex.framework.cloud.entities.BigDataTable;
import io.cloudex.framework.cloud.entities.QueryStats;
import io.cloudex.framework.cloud.entities.StorageObject;
import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.utils.ObjectUtils;
import io.ecarf.core.reason.rulebased.query.QueryGenerator;
import io.ecarf.core.term.TermUtils;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Config;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.TableUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * Reason task that saves all the inferred triples in each round in a single file then uploads it to Cloud storage then Big data. 
 * Hybrid big data streaming for inferred triples of 100,000 or smaller. This class reasons over compressed data. BigQuery results
 * exported to cloud storage files which are in compressed CSV format. This runs the reasoning process in parallel
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTask9 extends CommonTask {
	
	private final static Log log = LogFactory.getLog(DoReasonTask9.class);
	
	//private static final int MAX_CACHE = 40000000;
	
	//private int duplicates;
	
	private BigInteger totalRows = BigInteger.valueOf(0l);
	
	private Long totalBytes = 0L;
	
	private String table;
	
	// the encoded schema file
	private String schemaFile;
	
	// the encoded schema
	private String terms;
	
	// file if metadata is too long
	private String termsFile;
	
	private String bucket;
	
	// direct download rows limit
	protected int ddLimit;
	
	protected Map<Long, Set<Triple>> schemaTerms;
    
    protected ExecutorService executor;
    
    private Integer retries;
    
    private Integer sleep;

	/**
	 * Carryout the setup of the schema terms
	 * @param cloud
	 * @throws IOException
	 */
	private void setup(GoogleCloudService cloud) throws IOException {
	    
	    Set<String> termsSet;
        
        if(terms == null) {
            // too large, probably saved as a file

            log.info("Using json file for terms: " + termsFile);
            Validate.notNull(termsFile);
            
            String localTermsFile = Utils.TEMP_FOLDER + termsFile;
            cloud.downloadObjectFromCloudStorage(termsFile, localTermsFile, bucket);

            // convert from JSON
            termsSet = io.cloudex.framework.utils.FileUtils.jsonFileToSet(localTermsFile);
            
        } else {
            termsSet = ObjectUtils.csvToSet(terms);
        }
        
        String localSchemaFile = Utils.TEMP_FOLDER + schemaFile;
        // download the file from the cloud storage
        cloud.downloadObjectFromCloudStorage(schemaFile, localSchemaFile, bucket);

        // uncompress if compressed
        if(GzipUtils.isCompressedFilename(schemaFile)) {
            localSchemaFile = GzipUtils.getUncompressedFilename(localSchemaFile);
        }

        Map<Long, Set<Triple>> allSchemaTriples = 
                TripleUtils.getRelevantSchemaETriples(localSchemaFile, TermUtils.RDFS_TBOX);

        // get all the triples we care about
        schemaTerms = new HashMap<>();

        for(String termStr: termsSet) {
            
            Long term = Long.parseLong(termStr);
            
            if(allSchemaTriples.containsKey(term)) {
                schemaTerms.put(term, allSchemaTriples.get(term));
            }
        }
	    
	}
	
	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.Task#run()
	 */
	@Override
	public void run() throws IOException {
	    
	    GoogleCloudService cloud = (GoogleCloudService) this.getCloudService();

		Stopwatch stopwatch1 = Stopwatch.createUnstarted();
		Stopwatch stopwatch2 = Stopwatch.createUnstarted();
		
		this.setup(cloud);
		
		String decoratedTable = table;
		int emptyRetries = 0;
		int totalInferredTriples = 0;
		
		int maxRetries;
		if(this.retries == null) {
		    maxRetries = Config.getIntegerProperty(Constants.REASON_RETRY_KEY, 6);
		
		} else {
		    maxRetries = this.retries;
		}
		
		int cycleSleep;
		if(this.sleep == null) {
		    cycleSleep = Config.getIntegerProperty(Constants.REASON_SLEEP_KEY, 20);
		} else {
		    
		    cycleSleep = this.sleep;
		}
		
		this.ddLimit = Config.getIntegerProperty(Constants.REASON_DATA_DIRECT_DOWNLOAD_LIMIT, 1_200_000);
		int streamingThreshold = Config.getIntegerProperty("ecarf.io.reasoning.streaming.threshold", 100000);
		String instanceId = cloud.getInstanceId();
		
		int processors = Runtime.getRuntime().availableProcessors();
		
		if(processors > 1) {
		    this.executor = Utils.createFixedThreadPool(processors);
		}
		
		int count = 0;
		
		QueryGenerator<Long> generator = new QueryGenerator<Long>(schemaTerms, null);
		
		// timestamp loop
		do {	

			// First of all run all the queries asynchronously and remember the jobId and filename for each term
			generator.setDecoratedTable(decoratedTable);
			
			String query = generator.getQuery();
			log.debug("Generated Query: " + query);
			
			String queryResultFilePrefix = instanceId + "_QueryResults_" + count;

			String jobId = cloud.startBigDataQuery(query, new BigDataTable(this.table));
			//QueryResult	queryResult = QueryResult.create().setFilename(queryResultFilePrefix).setJobId(jobId);
			
			long start = System.currentTimeMillis();
			
			// block and wait for each job to complete then save results to a file
			QueryStats stats = cloud.saveBigQueryResultsToFile(jobId, queryResultFilePrefix, this.bucket, processors, this.ddLimit);
			
			BigInteger rows = stats.getTotalRows();
			
			this.totalBytes = this.totalBytes + stats.getTotalProcessedBytes();
			
			Set<Long> productiveTerms = new HashSet<>();
			Set<String> inferredTriplesFiles =  new HashSet<>();
			int interimInferredTriples = 0;
			
			// only process if triples are found matching this term
			if((rows != null) && !BigInteger.ZERO.equals(rows)) {

			    stopwatch1.start();

			    interimInferredTriples = this.inferAndSaveTriplesToFile(stats, productiveTerms, processors, inferredTriplesFiles);

			    this.totalRows = this.totalRows.add(rows);

			    stopwatch1.stop();

			} else {
			    log.info("Skipping query as no data is found");
			}

			totalInferredTriples += interimInferredTriples;

			if(interimInferredTriples > 0) {
				
				// stream smaller numbers of inferred triples
				// try uploading from cloud storage
				
				log.info("Inserting " + interimInferredTriples + 
						", inferred triples into Big Data table for " + productiveTerms.size() + " productive terms. Filename: " + inferredTriplesFiles);
				
				if(interimInferredTriples <= streamingThreshold) {
					// stream the data
					
					Set<Triple> inferredTriples = new HashSet<>();
					for(String inferredTriplesFile: inferredTriplesFiles) {
					     TripleUtils.loadCompressedCSVTriples(inferredTriplesFile, true, inferredTriples);
					}
					
					log.info("Total triples to stream into Big Data: " + inferredTriples.size());
					cloud.streamObjectsIntoBigData(inferredTriples, TableUtils.getBigQueryEncodedTripleTable(table));
					
					log.info("All inferred triples are streamed into Big Data table");
					
				} else {
					
				    List<String> cloudStorageFiles = new ArrayList<>();
					// load the data through cloud storage
					// upload the file to cloud storage
				    for(String inferredTriplesFile: inferredTriplesFiles) {
				        log.info("Uploading inferred triples file into cloud storage: " + inferredTriplesFile);
				        StorageObject file = cloud.uploadFileToCloudStorage(inferredTriplesFile, bucket);
				        log.info("File " + file + ", uploaded successfully. Now loading it into big data.");
				        cloudStorageFiles.add(file.getUri());
				    }
					
					jobId = cloud.loadCloudStorageFilesIntoBigData(cloudStorageFiles, TableUtils.getBigQueryEncodedTripleTable(table), false);
					
					log.info("All inferred triples are loaded into Big Data table through cloud storage, completed jobId: " + jobId);
					
				}
				
				// reset empty retries
				emptyRetries = 0;
				
				stopwatch2.reset();

			} else {
				log.info("No new inferred triples");
				// increment empty retries
				emptyRetries++;
				
				if(!stopwatch2.isRunning()) {
					stopwatch2.start();
				}
			}

			log.info("Total inferred triples so far = " + totalInferredTriples + ", current retry count: " + emptyRetries);
			
			if(emptyRetries < maxRetries) {
				ApiUtils.block(cycleSleep);

				// FIXME move into the particular cloud implementation service
				long elapsed = System.currentTimeMillis() - start;
				decoratedTable =  "[" + table + "@-" + elapsed + "-]";

				log.info("Using table decorator: " + decoratedTable + ". Empty retries count: " + emptyRetries);
			}
			
			count++;

		} while(emptyRetries < maxRetries); // end timestamp loop
		
		executor.shutdown();
		log.info("Finished reasoning, total inferred triples = " + totalInferredTriples);
		//log.info("Number of avoided duplicate terms = " + this.duplicates);
		log.info("Total rows retrieved from big data = " + this.totalRows);
		log.info("Total processed GBytes = " + ((double) this.totalBytes / FileUtils.ONE_GB));
		log.info("Total process reasoning time (serialization in inf file) = " + stopwatch1);
		log.info("Total time spent in empty inference cycles = " + stopwatch2);
	}
	

	/**
	 * 
	 * @param term
	 * @param select
	 * @param schemaTriples
	 * @param rows
	 * @param table
	 * @param inferredTriplesFiles 
	 * @param writer
	 * @return
	 * @throws IOException
	 */
	protected int inferAndSaveTriplesToFile(QueryStats stats, Set<Long> productiveTerms, int processors, Set<String> inferredTriplesFiles) throws IOException {

		log.info("********************** Starting Inference Round **********************");
		
		int inferredTriples = 0;
        
        boolean compressed = stats.getTotalRows().intValue() > this.ddLimit;
        
        List<String> files = stats.getOutputFiles();
        
        if(!files.isEmpty()) {

            String outFile;
            
            if((processors == 1) || (files.size() == 1)) {
                // if one file or one processor then reason serially 

                for(String file: files) {

                    outFile =  file + Constants.DOT_INF;

                    int inferred = ReasonUtils.reason(file, outFile, compressed,  schemaTerms, productiveTerms);

                    if(inferred > 0) {
                        inferredTriplesFiles.add(outFile);
                    }

                    inferredTriples += inferred;
                }

            } else {

                // multiple cores
                List<ReasonSubTask> tasks = new ArrayList<>();
                
                for(String file: files) {
                    tasks.add(new ReasonSubTask(compressed, file, schemaTerms));
                }
                
                try {
                    List<Future<ReasonResult>> results = executor.invokeAll(tasks);

                    for (Future<ReasonResult> result : results) {
                        ReasonResult reasonResult = result.get();
                        
                        outFile =  reasonResult.getOutFile();

                        int inferred = reasonResult.getInferred();
                        
                        productiveTerms.addAll(reasonResult.getProductiveTerms());

                        if(inferred > 0) {
                            inferredTriplesFiles.add(outFile);
                        }

                        inferredTriples += inferred;
                    }

                } catch(Exception e) {
                    log.error("Failed to run reasoning job in parallel", e);
                    executor.shutdown();
                    throw new IOException(e);
                }

            }
        }
        
		log.info("Total Rows: " + stats.getTotalRows() + 
				", Total Processed Bytes: " + stats.getTotalProcessedGBytes() + " GB" + 
				", Inferred: " + inferredTriples + ", compressed = " + compressed + 
				", out files: " + inferredTriplesFiles.size());

		log.info("********************** Completed Inference Round **********************");
		
		return inferredTriples;
	}


    /**
     * @return the table
     */
    public String getTable() {
        return table;
    }


    /**
     * @param table the table to set
     */
    public void setTable(String table) {
        this.table = table;
    }


    /**
     * @return the schemaFile
     */
    public String getSchemaFile() {
        return schemaFile;
    }


    /**
     * @param schemaFile the schemaFile to set
     */
    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }


    /**
     * @return the terms
     */
    public String getTerms() {
        return terms;
    }


    /**
     * @param terms the terms to set
     */
    public void setTerms(String terms) {
        this.terms = terms;
    }


    /**
     * @return the termsFile
     */
    public String getTermsFile() {
        return termsFile;
    }


    /**
     * @param termsFile the termsFile to set
     */
    public void setTermsFile(String termsFile) {
        this.termsFile = termsFile;
    }


    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }


    /**
     * @param bucket the bucket to set
     */
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }


    /**
     * @param schemaTerms the schemaTerms to set
     */
    protected void setSchemaTerms(Map<Long, Set<Triple>> schemaTerms) {
        this.schemaTerms = schemaTerms;
    }

    /**
     * @return the retries
     */
    public Integer getRetries() {
        return retries;
    }

    /**
     * @param retries the retries to set
     */
    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    /**
     * @return the sleep
     */
    public Integer getSleep() {
        return sleep;
    }

    /**
     * @param sleep the sleep to set
     */
    public void setSleep(Integer sleep) {
        this.sleep = sleep;
    }
	
}
