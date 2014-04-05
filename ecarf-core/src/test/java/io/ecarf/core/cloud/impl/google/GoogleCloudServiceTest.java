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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.ecarf.core.utils.Callback;
import io.ecarf.core.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
		assertTrue(outFile.endsWith("_out.gz"));
	}

	@Test
	public void testDownloadFileFromCloudStorage() throws IOException {
		this.service.inti();
		
		this.service.downloadObjectFromCloudStorage("linkedgeodata_links.nt.gz", 
				  Utils.TEMP_FOLDER + "/linkedgeodata_links.nt.gz", "ecarf", new Callback() {
					@Override
					public void execute() {
						System.out.println("Download complete");
						
					}
		});
	}
}
