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


package io.ecarf.core.cloud.task.processor.reason.phase3;

import static org.junit.Assert.*;
import io.ecarf.core.reason.rulebased.query.QueryGeneratorTest;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DuplicatesBusterTest {

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * Test method for {@link io.ecarf.core.cloud.task.processor.reason.phase3.DuplicatesBuster#isDuplicate(io.ecarf.core.triple.Triple)}.
     * @throws IOException 
     */
    @Test
    public void testIsDuplicate() throws IOException {
        URL dataUrl = QueryGeneratorTest.class.getResource("/data_enc.csv");
        
        List<Triple> triples = TripleUtils.csvToTriples(dataUrl.getPath(), true);
        
        List<Triple> rdfTypeTriples = new ArrayList<>();
        
        DuplicatesBuster buster = new DuplicatesBuster();
        
        for(Triple triple: triples) {
            assertFalse(buster.isDuplicate(triple));
            
            
            if(((Long) triple.getPredicate()) == 0) {
                rdfTypeTriples.add(triple);
            }
        }
        
        int duplicates = 0;
        
        for(Triple triple: triples) {
            boolean duplicate = buster.isDuplicate(triple);
            
            if(rdfTypeTriples.contains(triple)) {
                assertTrue(duplicate);
            } else {
                assertFalse(duplicate);
            }
            
            if(duplicate) {
                duplicates++;
            }
        }
        
        assertEquals(rdfTypeTriples.size(), duplicates);
        
        
    }

}
