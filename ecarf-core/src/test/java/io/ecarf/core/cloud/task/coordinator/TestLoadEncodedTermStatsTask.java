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


package io.ecarf.core.cloud.task.coordinator;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import io.cloudex.framework.partition.entities.Item;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.utils.TestUtils;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TestLoadEncodedTermStatsTask {
    
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
     * Test method for {@link io.ecarf.core.cloud.task.coordinator.LoadEncodedTermStatsTask#run()}.
     * @throws IOException 
     */
    @Test
    public void testRun() throws IOException {
        LoadEncodedTermStatsTask task = new LoadEncodedTermStatsTask();
        task.setCloudService(service);
        task.setBucket("swetodblp-fullrun-1");
        task.setEncodedTermStatsFile("term_stats_encoded.json");
        task.run();
        
        Map<String, Object> output = task.getOutput();
        
        assertNotNull(output);
        assertEquals(1, output.size());
        
        List<Item> items = (List<Item>) output.get("termItems");
        
        assertNotNull(items);
        
        assertEquals(28, items.size());
    }

}
