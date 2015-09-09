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


package io.ecarf.core.cloud.task.processor.analyze;

import static org.junit.Assert.assertFalse;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.utils.TestUtils;

import java.io.IOException;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ExtractAndCountTermsTaskTest {


    private EcarfGoogleCloudServiceImpl service;

    /**
     * @throws java.lang.Exception
     */
    //@Before
    public void setUp() throws Exception {
        this.service = new EcarfGoogleCloudServiceImpl();
        TestUtils.prepare(service);
    }
    /**
     * Test method for {@link io.ecarf.core.cloud.task.processor.analyze.ExtractAndCountTermsTask#run()}.
     * @throws IOException 
     */
    //@Test
    public void testRun() throws IOException {
        ExtractAndCountTermsTask task = new ExtractAndCountTermsTask();
        task.setCloudService(service);
        task.setBucket("swetodblp1");
        task.setFiles("swetodblp_2008_1.nt.gz,"
                + "swetodblp_2008_2.nt.gz,"
                + "swetodblp_2008_3.nt.gz,"
                + "swetodblp_2008_4.nt.gz,"
                + "swetodblp_2008_5.nt.gz,"
                + "swetodblp_2008_6.nt.gz,"
                + "swetodblp_2008_7.nt.gz,"
                + "swetodblp_2008_8.nt.gz");
        task.setSchemaTermsFile("schema_terms.json");
        
        task.run();
        
        assertFalse(task.getAllTerms().isEmpty());
    }
    
    public static void main(String[] args) throws Exception {
        ExtractAndCountTermsTaskTest test = new ExtractAndCountTermsTaskTest();
        
        test.setUp();
        test.testRun();
    }

}
