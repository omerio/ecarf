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


package io.ecarf.core.cloud.task.processor.dictionary;

import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.utils.TestUtils;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class AssembleDictionaryTaskTest {
    
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
     * Test method for {@link io.ecarf.core.cloud.task.coordinator.CombineTermStatsTask#run()}.
     * @throws IOException 
     */
    @Test
    public void testRun() throws IOException {
        
        AssembleDictionaryTask task = new AssembleDictionaryTask();
        
        task.setCloudService(service);
        task.setBucket("swetodblp-term-2part--1");
        task.setTargetBucket("swetodblp-local");
        task.setSchemaBucket("swetodblp1");
        task.setSchemaFile("opus_august2007_closure.nt");
        task.setTermStatsFile("term_stats.json");
        task.setEncodedSchemaFile("opus_august2007_closure_encoded.csv");
        
        task.setEncodedTermStatsFile("term_stats_encoded.json");
        
        
        task.run();
        
    }

}
