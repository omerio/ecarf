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
import io.ecarf.core.cloud.VMConfig;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.types.TaskType;
import io.ecarf.core.utils.Callback;
import io.ecarf.core.utils.Constants;
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
	
	private GoogleCloudService service;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.service = new GoogleCloudService();
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
		URL url = this.getClass().getResource("/linkedgeodata_links.nt.gz");
		File inputFile = new File(url.toURI());
		System.out.println(inputFile.getAbsolutePath());
		String outFile = this.service.prepareForCloudDatabaseImport(inputFile.getAbsolutePath());
		assertNotNull(outFile);
		assertTrue(outFile.endsWith("_out.gz")); //
	}

	@Test
	@Ignore
	public void testDownloadFileFromCloudStorage() throws IOException {
		this.service.inti();
		
		this.service.downloadObjectFromCloudStorage("linkedgeodata_links.nt.gz", 
				Utils.TEMP_FOLDER + "linkedgeodata_links.nt.gz", "ecarf", new Callback() {
			@Override
			public void execute() {
				System.out.println("Download complete");

			}
		});
	}
	
	@Test
	@Ignore
	public void testUploadFileToCloudStorage() throws IOException {

		this.service.uploadFileToCloudStorage(Utils.TEMP_FOLDER + "umbel_links.nt_out.gz", "ecarf",  new Callback() {
			
			@Override
			public void execute() {
				System.out.println("Upload complete");
			}
		});
	}
	
	@Test
	@Ignore
	public void testRunBigDataQuery() throws IOException {
		 String query = //"SELECT TOP( title, 10) as title, COUNT(*) as revision_count "
			        //+ "FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;";
				 "select subject from swetodlp.swetodlp_triple where " +
				 "object = \"<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>\";";
		 
		this.service.runBigDataQuery(query, System.out);
	}
	
	@Test
	@Ignore
	public void testStartBigDataQuery() throws IOException {
		String query = //"SELECT TOP( title, 10) as title, COUNT(*) as revision_count "
		        //+ "FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;";
			 "select subject from swetodlp.swetodlp_triple where " +
			 "object = \"<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>\";";
		
		String jobId = this.service.startBigDataQuery(query);
		String completedJob = this.service.checkBigQueryJobResults(jobId, false, true);
		this.service.displayQueryResults(completedJob);
	}
	
	@Test
	//@Ignore
	public void testSaveBigDataToFile() throws IOException {
		String query = "SELECT TOP( title, 20) as title, COUNT(*) as revision_count "
		        + "FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;";
			// "select subject from swetodlp.swetodlp_triple where " +
			 //"object = \"<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>\";";
		
		String jobId = this.service.startBigDataQuery(query);
		String filename = Utils.TEMP_FOLDER + 
				Utils.encodeFilename("<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>") + 
				Constants.DOT_TERMS;
		
		BigInteger rows = this.service.saveBigQueryResultsToFile(jobId, filename);
		System.err.println(rows);
		
		
	}
	
	@Test
	@Ignore
	public void testLoadCloudStorageFilesIntoBigData() throws IOException {
		String jobId = this.service.loadCloudStorageFilesIntoBigData(Arrays.asList("gs://ecarf/umbel_links.nt_out.gz", 
				"gs://ecarf/yago_links.nt_out.gz"), "swetodlp.test", false);
		assertNotNull(jobId);
		System.out.println(jobId);
	}
	
	@Test
	@Ignore
	public void testLoadLocalFilesIntoBigData() throws IOException {
		List<String> jobIds = this.service.loadLocalFilesIntoBigData(
				Arrays.asList("/Users/omerio/Downloads/umbel_links.nt_out.gz", 
						"/Users/omerio/Downloads/linkedgeodata_links.nt_out.gz"), "swetodlp.test", false);
		assertNotNull(jobIds);
		System.out.println(jobIds);
	}
	
	
	
	
	@Test
	@Ignore
	public void testDownloadFileFromCloudStorage1() throws IOException {
		this.service.setAccessToken(TestUtils.TOKEN);
		this.service.setTokenExpire(DateUtils.addHours(new Date(), 1));
		
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
		
		
		VMMetaData metaData = new VMMetaData();
		//metaData.addValue(VMMetaData.ECARF_TASK, TaskType.LOAD.toString())
			//.addValue(VMMetaData.ECARF_FILES, "file1.txt, file2.txt");
		
		VMConfig conf = new VMConfig();
		conf.setImageId("centos-cloud/global/images/centos-6-v20140408")
			.setInstanceId("ecarf-evm-1")
			.setMetaData(metaData)
			.setNetworkId("default")
			.setVmType("f1-micro");
		
		boolean success = this.service.startInstance(Lists.newArrayList(conf), true);
		
		assertEquals(true, success);
		
	}
	
	@Test
	@Ignore
	public void testShutdownInstance() throws IOException {
		
		VMConfig conf = new VMConfig();
		conf.setInstanceId("ecarf-evm-2");
		
		this.service.shutdownInstance(Lists.newArrayList(conf));
	}
	
	@Test
	@Ignore
	public void testInstanceMetadata() throws IOException {
		
		// read the current metadata
		VMMetaData metadata = this.service.getEcarfMetaData(null, null);
		assertNotNull(metadata);
		assertNotNull(metadata.getFingerprint());
		assertEquals(TaskType.LOAD, metadata.getTaskType());
		assertEquals(4, metadata.getAttributes().size());
		
		// now update the task type
		metadata.clearValues();
		metadata.addValue(VMMetaData.ECARF_TASK, TaskType.REASON.toString());
		this.service.updateInstanceMetadata(metadata);
		assertEquals(TaskType.REASON, metadata.getTaskType());
		assertEquals(1, metadata.getAttributes().size());
	}
	
	
}
