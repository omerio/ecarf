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

import static org.junit.Assert.*;
import io.ecarf.core.cloud.VMConfig;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.types.TaskType;
import io.ecarf.core.utils.Callback;
import io.ecarf.core.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;

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
	public void testDownloadFileFromCloudStorage1() throws IOException {
		this.service.setAccessToken("ya29.1.AADtN_XP6QG8iTv295o11ozn0sAJAmvS2TWsj5fiUIRafCcULqQLd7-jn_KERBoLGtQdM9g");
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
		
		this.prepare();
		VMMetaData metaData = new VMMetaData();
		metaData.addValue(VMMetaData.ECARF_TASK, TaskType.LOAD.toString())
			.addValue(VMMetaData.ECARF_FILES, "file1.txt, file2.txt");
		
		VMConfig conf = new VMConfig();
		conf.setImageId("centos-cloud/global/images/centos-6-v20140318")
			.setInstanceId("ecarf-evm-2")
			.setMetaData(metaData)
			.setNetworkId("default")
			.setVmType("f1-micro");
		
		boolean success = this.service.startInstance(Lists.newArrayList(conf), true);
		
		assertEquals(true, success);
		
	}
	
	@Test
	//@Ignore
	public void testShutdownInstance() throws IOException {
		this.prepare();
		VMConfig conf = new VMConfig();
		conf.setInstanceId("ecarf-evm-2");
		
		this.service.shutdownInstance(Lists.newArrayList(conf));
	}
	
	private void prepare() {
		this.service.setAccessToken("ya29.1.AADtN_UqJQO1oTpM7uP7HR_S-E_7ivhaLnM5ngNiF5v6xdS6fGlcyKfwk9lGgTf9w8X-7is");
		this.service.setTokenExpire(DateUtils.addHours(new Date(), 1));
		this.service.setProjectId("ecarf-1000");
		this.service.setZone("us-central1-a");
		this.service.setServiceAccount("default");
		this.service.setScopes(Lists.newArrayList("https://www.googleapis.com/auth/userinfo.email",
	        "https://www.googleapis.com/auth/compute",
	        "https://www.googleapis.com/auth/devstorage.full_control",
	        "https://www.googleapis.com/auth/bigquery"));
	}
	
	
}
