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
import io.ecarf.core.triple.ETriple;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTask7Test {
    
    private static final Map<String, Long> dictionary = new HashMap<>();
    
    static {
        dictionary.put("<http://www.mcgill.ca/>", 3138381L);
        dictionary.put("<http://www.uni-trier.de/>", 776045L);
        dictionary.put("<http://www.lancs.ac.uk/>", 6883397L);
        dictionary.put("<http://www.wisc.edu/>", 5931373L);
        dictionary.put("<http://www.ucsb.edu/>", 2636789L);
        
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/journals/ijprai/Robertson97>", 28087695658456L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/journals/ijprai/SiromoneyHCS92>", 27345940339864L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/journals/ijprai/SiromoneyMSD94>", 28087695658392L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/journals/ijprai/RiantoKK00>", 27350222461128L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/journals/ijprai/Singh99>", 14912065592728L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/journals/ijprai/RiceKN94>", 27345940339160L);
        
        dictionary.put("<http://lsdis.cs.uga.edu/projects/semdis/opus#Publication>", 71752386272432L);
        
        dictionary.put("<http://xmlns.com/foaf/0.1/Organization>", 74632293448224L);
        dictionary.put("<http://xmlns.com/foaf/0.1/Document>", 7653280640864L);
        
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/ms/Ley2006>", 5711498261776L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/ms/Vollmer2006>", 4560375739408L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/ms/Yurek97>", 1514437763392L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/ms/Klaas2007>", 1313652024656L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/ms/Brown92>", 212844377104L);
        
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/phd/Shang94>", 5659615978232L);
        
        dictionary.put(SchemaURIType.RDF_TYPE.uri, (long) SchemaURIType.RDF_TYPE.id);
        
        dictionary.put(SchemaURIType.OWL_TOP_DATA_PROPERTY.uri, (long) SchemaURIType.OWL_TOP_DATA_PROPERTY.id);
        

        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/journals/iandc/AulettaP01>", 18047871528728L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/journals/jucs/Skillicorn97>", 249868508640L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/journals/ai/SmithA08>", 18945063962208L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/conf/ocl/RichtersG02>", 18640934414264L);
        dictionary.put("<http://dblp.uni-trier.de/rec/bibtex/books/acm/kim95/AnnevelinkACFHK95>", 26885349335728L);

        
    }

    @Test
    public void testInferAndSaveTriplesToFile() throws FileNotFoundException, IOException {
        this.doTestInferAndSaveTriplesToFile("/schema.csv", "/inferred.nt");
        this.doTestInferAndSaveTriplesToFile("/schema1.csv", "/inferred1.nt");
    }

    /**
     * Test method for {@link io.ecarf.core.cloud.task.processor.reason.phase2.DoReasonTask7
     * #inferAndSaveTriplesToFile(io.ecarf.core.cloud.task.processor.reason.phase2.QueryResult, 
     * java.util.Set, java.lang.String, java.io.PrintWriter)}.
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    private void doTestInferAndSaveTriplesToFile(String schemaFile, String inferredFile) throws FileNotFoundException, IOException {
        
        URL schemaUrl = QueryGeneratorTest.class.getResource(schemaFile);
        URL dataUrl = QueryGeneratorTest.class.getResource("/data_enc.csv");
        URL inferredUrl = QueryGeneratorTest.class.getResource(inferredFile);
        
        DoReasonTask7 task = new DoReasonTask7(); 
        
        Map<Long, Set<Triple>> schemaTriples = 
                TripleUtils.getRelevantSchemaETriples(schemaUrl.getPath(), TermUtils.RDFS_TBOX);
        
        task.setSchemaTerms(schemaTriples);
        
        //QueryResult queryResult, Set<String> productiveTerms, String table, PrintWriter writer
        QueryResult queryResult = new QueryResult();
        queryResult.setFilename(dataUrl.getPath());
        queryResult.setStats(new QueryStats());
        
        Set<Long> productiveTerms = new HashSet<>();
        String table = "my-table-1";
        
        //PrintWriter writer = new PrintWriter(System.out);
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        
        int numInferred = task.inferAndSaveTriplesToFile(queryResult, productiveTerms, table, writer);
        
        writer.flush();
        String inferredStr = stringWriter.toString();
        System.out.println(inferredStr);
        StringReader reader = new StringReader(inferredStr);
        List<Triple> inferred = TripleUtils.csvToTriples(reader, true);
        List<Triple> jenaNt = new ArrayList<>();
        List<Triple> jena = new ArrayList<>();
        TripleUtils.loadNTriples(inferredUrl.getPath(), jenaNt);
        
        for(Triple triple: jenaNt) {
            ETriple eTriple = new ETriple();
            eTriple.setSubject(dictionary.get(triple.getSubject()));
            eTriple.setPredicate(dictionary.get(triple.getPredicate()));
            
            String object = (String) triple.getObject();
            
            if(object.startsWith("<")) {
                
                eTriple.setObject(dictionary.get(object));
                
            } else {
                // literal
                eTriple.setObjectLiteral(object);
            }
            
            jena.add(eTriple);
        }
        
        assertFalse(inferred.isEmpty());
        assertFalse(jena.isEmpty());
        assertEquals(numInferred, inferred.size());
        
        for(int i = 0; i < jena.size(); i++) {
            Triple inf = inferred.get(i);
            Triple act = jena.get(i);
            
            assertEquals(act, inf);
        }
    }

}
