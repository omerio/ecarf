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
package io.ecarf.core.cloud.impl.google;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.cloudex.framework.cloud.api.Callback;
import io.cloudex.framework.cloud.entities.StorageObject;
import io.cloudex.framework.cloud.entities.VmMetaData;
import io.cloudex.framework.config.VmConfig;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.TableUtils;
import io.ecarf.core.utils.TestUtils;
import io.ecarf.core.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Lists;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
@RunWith(JUnit4.class)
public class GoogleCloudServiceTest {
	
	private EcarfGoogleCloudServiceImpl service;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.service = new EcarfGoogleCloudServiceImpl();
		TestUtils.prepare(service);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link io.ecarf.core.cloud.impl.google.GoogleCloudService#
	 * prepareForCloudDatabaseImport(java.lang.String)}.
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	@Test
	@Ignore
	public void testPrepareForCloudDatabaseImport() throws IOException, URISyntaxException {
		URL url = this.getClass().getResource("/gutenberg_links.nt.gz");
		File inputFile = new File(url.toURI());
		System.out.println(inputFile.getAbsolutePath());
		String outFile = this.service.prepareForBigQueryImport(inputFile.getAbsolutePath());
		assertNotNull(outFile);
		assertTrue(outFile.endsWith("_out.gz")); //
	}
	
	/*@Test
	public void testGetCompletedBigQueryJob() throws IOException, URISyntaxException {
		
		URL url = this.getClass().getResource("/bigquery_jobIds.txt");
		File inputFile = new File(url.toURI());
		boolean skipRowQuery = false;
		
		int noOfJobs = 0;
		BigInteger totalRows = BigInteger.ZERO;
		double totalProcessedBytes = 0d;
		
		try (BufferedReader reader = new BufferedReader(new FileReader(inputFile.getAbsolutePath()), Constants.GZIP_BUF_SIZE)) {
			String line;
			while ((line = reader.readLine()) != null) {
				Job job = this.service.getCompletedBigQueryJob(line, false);
				if(job != null) {
					
					noOfJobs++;
					
					System.out.println("Job Id: " + line);
					String completedJobId = job.getJobReference().getJobId();
					
					//job.getStatistics().get
					
					JobStatistics stats = job.getStatistics();
					JobConfiguration config = job.getConfiguration();
					// log the query
					if(config != null) {
						System.out.println("query: " + (config.getQuery() != null ? config.getQuery().getQuery() : ""));
					}
					// log the total bytes processed
					JobStatistics2 qStats = stats.getQuery();
					if(qStats != null) {
						double processedBytes = ((double) qStats.getTotalBytesProcessed() / FileUtils.ONE_GB);
						
						totalProcessedBytes = totalProcessedBytes + processedBytes;
						
						System.out.println("Total Bytes processed: " + processedBytes + " GB");
						System.out.println("Cache hit: " + qStats.getCacheHit());
					}
					
					JobStatistics3 lStats = stats.getLoad();
					if(lStats != null) {
						System.out.println("Output rows: " + lStats.getOutputRows());
						
					}
					
					long time = stats.getEndTime() - stats.getCreationTime();
					System.out.println("Elapsed query time (ms): " + time);
					System.out.println("Elapsed query time (s): " + TimeUnit.MILLISECONDS.toSeconds(time));
					
					// total rows found
					try {
						if(!skipRowQuery) {
							GetQueryResultsResponse queryResult = this.service.getQueryResults(completedJobId, null);

							BigInteger rows = queryResult.getTotalRows();
							System.out.println("Total rows found for the query: " + rows);

							totalRows = totalRows.add(rows);
						}
					} catch(Exception e) {
						e.printStackTrace();
					}
					

				}
				System.out.println("Total number of jobs so far : " + noOfJobs);
				System.out.println("Total processed bytes so far: " + totalProcessedBytes + "GB");
				if(!skipRowQuery) {
					System.out.println("Total rows for the all queries so far: " + totalRows);
				}
				System.out.println("----------------------------------------------------------------------------------------------------------------------------------------------------------");
			}
		}
		
		System.out.println("Total number of job: " + noOfJobs);
		System.out.println("Total processed bytes: " + totalProcessedBytes + "GB");
		if(!skipRowQuery) {
			System.out.println("Total rows for the all queries: " + totalRows);
		}
	}*/
	
	/**
	 * Test method for {@link io.ecarf.core.cloud.impl.google.GoogleCloudService#
	 * prepareForCloudDatabaseImport(java.lang.String)}.
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	@Test
	@Ignore
	public void testPrepareForCloudDatabaseImport1() throws IOException, URISyntaxException {
		/*URL url = this.getClass().getResource("/linkedgeodata_links.nt.gz");
		File inputFile = new File(url.toURI());
		System.out.println(inputFile.getAbsolutePath());*/
		String outFile = this.service.prepareForBigQueryImport("/Users/omerio/Ontologies/dbpedia/tbox/ntriples.nt.gz");
		assertNotNull(outFile);
		assertTrue(outFile.endsWith("_out.gz")); //
	}

	@Test
	public void testDownloadFileFromCloudStorage() throws IOException {
		this.service.init();
		
		this.service.downloadObjectFromCloudStorage("swetodblp_06012015000000000011", 
				Utils.TEMP_FOLDER + "swetodblp_06012015000000000011", "ecarf", new Callback() {
			@Override
			public void execute() {
				System.out.println("Download complete");

			}
		});
	}
	
	@Test
	@Ignore
	public void testUploadFileToCloudStorage() throws IOException {

		StorageObject file = this.service.uploadFileToCloudStorage("/Users/omerio/Ontologies/dbpedia/yago_taxonomy.nt", "ecarf",  new Callback() {
			
			@Override
			public void execute() {
				System.out.println("Upload complete");
			}
		});
		
		System.out.println(file);
	}
	

	//@Test
	//@Ignore
	/*public void testStartBigDataQuery() throws IOException {
		String query = //"SELECT TOP( title, 10) as title, COUNT(*) as revision_count "
		        //+ "FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;";
			 "select subject from swetodlp.swetodlp_triple where " +
			 "object = \"<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>\";";
		
		String jobId = this.service.startBigDataQuery(query);
		String completedJob = this.service.checkBigQueryJobResults(jobId, false, true);
		this.service.displayQueryResults(completedJob);
	}*/
	
	@Test
	@Ignore
	public void testSaveBigDataToFile() throws IOException {
		String query = //"SELECT TOP( title, 20) as title, COUNT(*) as revision_count "
		        //+ "FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;";
			"select subject from ontologies.swetodblp where " +
			"object = \"<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>\";";
		
		String jobId = this.service.startBigDataQuery(query);
		String filename = Utils.TEMP_FOLDER + 
				io.cloudex.framework.utils.FileUtils.encodeFilename("<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>") + 
				Constants.DOT_TERMS;
		
		BigInteger rows = this.service.saveBigQueryResultsToFile(jobId, filename).getTotalRows();
		System.err.println(rows);
		
		
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	@Test
	@Ignore
	public void testStreamDataIntoBigQuery() throws IOException {
		//gutenberg_links.nt
		// load the triples from a file
		Set<Triple> triples = TripleUtils.loadNTriples("/Users/omerio/Ontologies/dbpedia/yago_taxonomy.nt");
		System.out.println("Number of triples: " + triples.size());
		this.service.streamObjectsIntoBigData(triples, TableUtils.getBigQueryTripleTable("ontologies.test"));
	}
	
	@Test
	@Ignore
	public void testLoadCloudStorageFilesIntoBigData() throws IOException {
		String jobId = this.service.loadCloudStorageFilesIntoBigData(Arrays.asList("gs://ecarf/umbel_links.nt_out.gz", 
				"gs://ecarf/yago_links.nt_out.gz"), TableUtils.getBigQueryTripleTable("swetodlp.test"), false);
		assertNotNull(jobId);
		System.out.println(jobId);
	}
	
	@Test
	@Ignore
	public void testLoadLocalFilesIntoBigData() throws IOException {
		List<String> jobIds = this.service.loadLocalFilesIntoBigData(
				Arrays.asList("/Users/omerio/Downloads/umbel_links.nt_out.gz", 
						"/Users/omerio/Downloads/linkedgeodata_links.nt_out.gz"), 
						TableUtils.getBigQueryTripleTable("swetodlp.test"), false);
		assertNotNull(jobIds);
		System.out.println(jobIds);
	}
	
	
	
	
	@Test
	@Ignore
	public void testDownloadFileFromCloudStorage1() throws IOException {
		//this.service.setAccessToken(TestUtils.TOKEN);
		//this.service.setTokenExpire(DateUtils.addHours(new Date(), 1));
		
		this.service.downloadObjectFromCloudStorage("linkedgeodata_links.nt.gz", 
				Utils.TEMP_FOLDER + "/linkedgeodata_links.nt.gz", "ecarf", new Callback() {
			@Override
			public void execute() {
				System.out.println("Download complete");

			}
		});
	}
	
	@Test
	@Ignore
	public void testCreateInstance() throws IOException {
		
		//metaData.addValue(VMMetaData.ECARF_TASK, TaskType.LOAD.toString())
			//.addValue(VMMetaData.ECARF_FILES, "file1.txt, file2.txt");
		
		VmConfig conf = new VmConfig();
		conf.setImageId("centos-cloud/global/images/centos-6-v20140718");
		conf.setInstanceId("ecarf-test-vm");
		conf.setMetaData(new VmMetaData());
		conf.setNetworkId("default");
		conf.setVmType("n1-standard-1");
		conf.setDiskType("pd-ssd");
		
		boolean success = this.service.startInstance(Lists.newArrayList(conf), true);
		
		assertEquals(true, success);
		
	}
	
	@Test
	@Ignore
	public void testShutdownInstance() throws IOException {
		
		VmConfig conf = new VmConfig();
		conf.setInstanceId("ecarf-evm-2");
		
		this.service.shutdownInstance(Lists.newArrayList(conf));
	}
	
	@Test
	@Ignore
	public void testInstanceMetadata() throws IOException {
		
		// read the current metadata
		VmMetaData metadata = this.service.getMetaData(null, null);
		assertNotNull(metadata);
		assertNotNull(metadata.getFingerprint());
		assertEquals(4, metadata.getAttributes().size());
		
		// now update the task type
		metadata.clearValues();
		metadata.addValue(VmMetaData.CLOUDEX_TASK_CLASS, "blah.blah.BlahClass");
		this.service.updateMetadata(metadata);
		assertEquals(1, metadata.getAttributes().size());
	}
	
	
}
