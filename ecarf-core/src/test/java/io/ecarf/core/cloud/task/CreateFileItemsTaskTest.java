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
import io.cloudex.framework.partition.entities.Item;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.cloud.task.coordinator.CreateFileItemsTask;
import io.ecarf.core.utils.TestUtils;

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class CreateFileItemsTaskTest {

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
     * Test method for {@link io.ecarf.core.cloud.task.coordinator.CreateFileItemsTask#run()}.
     * @throws IOException 
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRun() throws IOException {

        CreateFileItemsTask task = new CreateFileItemsTask();
        task.setCloudService(service);
        task.setBucket("dbpedia");
        task.run();

        List<Item> items = (List<Item>) task.getOutput().get("fileItems");

        /*List<String> nodeFiles = results.getBinItems();

        for(String files: nodeFiles) {
            System.out.println("Partitioned files: " + files + "\n");
        }

        assertNotNull(nodeFiles);
        assertEquals(10, nodeFiles.size());

        Set<String> allFiles = new HashSet<>();

        for(String file: nodeFiles) {
            allFiles.addAll(Arrays.asList(StringUtils.split(file, ',')));
        }*/
        assertNotNull(items);

        assertEquals(64, items.size());
    }

}
