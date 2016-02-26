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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import io.cloudex.framework.cloud.entities.QueryStats;
import io.ecarf.core.reason.rulebased.query.QueryGeneratorTest;
import io.ecarf.core.term.TermUtils;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTask6Test {


    @Test
    public void testInferAndSaveTriplesToFile() throws FileNotFoundException, IOException {
        this.doTestInferAndSaveTriplesToFile("/schema.nt", "/inferred.nt");
        this.doTestInferAndSaveTriplesToFile("/schema1.nt", "/inferred1.nt");
    }

    /**
     * Test method for {@link io.ecarf.core.cloud.task.processor.reason.phase2.DoReasonTask6#inferAndSaveTriplesToFile(
     * io.ecarf.core.cloud.task.processor.reason.phase2.QueryResult, java.util.Set, java.lang.String, java.io.PrintWriter)}.
     * @throws IOException 
     */
    private void doTestInferAndSaveTriplesToFile(String schemaFile, String inferredFile) throws IOException {
        
        
        URL schemaUrl = QueryGeneratorTest.class.getResource(schemaFile);
        URL dataUrl = QueryGeneratorTest.class.getResource("/data.csv");
        URL inferredUrl = QueryGeneratorTest.class.getResource(inferredFile);
        
        DoReasonTask6 task = new DoReasonTask6(); 
        
        Map<String, Set<Triple>> schemaTriples = 
                TripleUtils.getRelevantSchemaNTriples(schemaUrl.getPath(), TermUtils.RDFS_TBOX);
        
        task.setSchemaTerms(schemaTriples);
        
        //QueryResult queryResult, Set<String> productiveTerms, String table, PrintWriter writer
        QueryResult queryResult = new QueryResult();
        queryResult.setFilename(dataUrl.getPath());
        queryResult.setStats(new QueryStats());
        
        Set<String> productiveTerms = new HashSet<>();
        String table = "my-table-1";
        
        //PrintWriter writer = new PrintWriter(System.out);
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        
        int numInferred = task.inferAndSaveTriplesToFile(queryResult, productiveTerms, table, writer);
        
        writer.flush();
        String inferredStr = stringWriter.toString();
        System.out.println(inferredStr);
        StringReader reader = new StringReader(inferredStr);
        List<Triple> inferred = TripleUtils.csvToTriples(reader, false);
        List<Triple> jena = new ArrayList<>();
        TripleUtils.loadNTriples(inferredUrl.getPath(), jena);
        
        assertFalse(inferred.isEmpty());
        assertFalse(jena.isEmpty());
        assertEquals(numInferred, inferred.size());
        
        Set<String> terms = new HashSet<>();
        for(int i = 0; i < jena.size(); i++) {
            Triple inf = inferred.get(i);
            Triple act = jena.get(i);
            
            assertEquals(act, inf);
            
            terms.add((String) act.getSubject());
            terms.add((String) act.getPredicate());
            terms.add((String) act.getObject());
         
        }
        
        for(String term: terms) {
            System.out.println("dictionary.put(\"" + term + "\", 1000000000L);");
        }
    }

}
