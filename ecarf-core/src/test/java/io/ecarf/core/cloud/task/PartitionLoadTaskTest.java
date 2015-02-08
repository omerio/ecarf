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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.impl.google.GoogleCloudService;
import io.ecarf.core.cloud.task.impl.partition.PartitionLoadTask;
import io.ecarf.core.utils.TestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class PartitionLoadTaskTest {
	
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
	 * Test method for {@link io.ecarf.core.cloud.task.impl.partition.PartitionLoadTask#run()}.
	 * @throws IOException 
	 */
	@Test
	public void testRun() throws IOException {
		
		Input input = (new Input()).setBucket("dbpedia").setWeightPerNode(509643034L)
				.setNewBinPercentage(0.0);
		PartitionLoadTask task = new PartitionLoadTask(null, service);
		task.setInput(input);
		task.run();
		
		Results results = task.getResults();
		
		List<String> nodeFiles = results.getBinItems();
		
		for(String files: nodeFiles) {
			System.out.println("Partitioned files: " + files + "\n");
		}
		
		assertNotNull(nodeFiles);
		assertEquals(10, nodeFiles.size());
		
		Set<String> allFiles = new HashSet<>();
		
		for(String file: nodeFiles) {
			allFiles.addAll(Arrays.asList(StringUtils.split(file, ',')));
		}
		
		assertEquals(64, allFiles.size());
	}

}
