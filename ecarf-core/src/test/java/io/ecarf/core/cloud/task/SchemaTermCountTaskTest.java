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
package io.ecarf.core.cloud.task;

import static org.junit.Assert.*;

import java.io.IOException;

import io.ecarf.core.cloud.TestUtils;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.impl.google.GoogleCloudService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class SchemaTermCountTaskTest {
	
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
	 * Test method for {@link io.ecarf.core.cloud.task.SchemaTermCountTask#run()}.
	 * @throws IOException 
	 */
	@Test
	public void testRun() throws IOException {
		VMMetaData metadata = new VMMetaData();
		metadata.addValue(VMMetaData.ECARF_BUCKET, "ecarf");
		metadata.addValue(VMMetaData.ECARF_SCHEMA, "dbpedia_3.9_gen_closure.nt");
		SchemaTermCountTask task = new SchemaTermCountTask(metadata, service);
		task.run();
	}

}
