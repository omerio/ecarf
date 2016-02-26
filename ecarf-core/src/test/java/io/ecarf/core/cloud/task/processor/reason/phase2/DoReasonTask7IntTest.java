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


package io.ecarf.core.cloud.task.processor.reason.phase2;

import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.utils.TestUtils;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTask7IntTest {
    
    private EcarfGoogleCloudServiceImpl service;
    
    @Before
    public void setUp() throws Exception {
       
        this.service = new EcarfGoogleCloudServiceImpl();
        TestUtils.prepare(service);
    }

    /**
     * Test method for {@link io.ecarf.core.cloud.task.processor.reason.phase2.DoReasonTask7#run()}.
     * @throws IOException 
     */
    @Test
    public void testRun() throws IOException {
        DoReasonTask7 task = new DoReasonTask7();
        task.setCloudService(service);
        task.setBucket("swetodblp-fullrun-2");
        task.setTable("ontologies.swetodblp1");
        task.setSchemaFile("opus_august2007_closure_encoded.csv");
        task.setTerms("99714255280,46866513000000,357392157088,1200228370848,79293028576,306863398384");//,"
                    //+ "370280483312,27422070944,369944612064,95080668400,83256854752,306925456608,"
                    //+ "285204270496,30705355936,352848615584,306929376416,1110018005408,83335567856,"
                    //+ "27706181856,4412645760496,30638312928,5511101257888,1406102575520,1113037199584,"
                    //+ "1127201020336,100767866352,17893680928928,354175405552");
        task.run();
    }

}
