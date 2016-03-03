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


package io.ecarf.core.reason.rulebased.query;

import static org.junit.Assert.assertEquals;
import io.ecarf.core.term.TermUtils;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
//import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class QueryGeneratorTest {

    private static final String Q_PART_1 = "select subject, predicate, object from ";
    
    private static final String Q_PART_1_A = "select subject, predicate, object, object_literal from ";
    
    private static final String Q_PART_2 = " where (object=\"<http://lsdis.cs.uga.edu/projects/semdis/opus#Article>\" "
            + "and predicate=\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\") OR "
            + "(predicate IN (\"<http://lsdis.cs.uga.edu/projects/semdis/opus#at_organization>\",\"<http://lsdis.cs.uga.edu/projects/semdis/opus#pages>\"));";
    
    private static final String Q_PART_2_A = " where (predicate=\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\" "
            + "and object IN (\"<http://lsdis.cs.uga.edu/projects/semdis/opus#Book>\",\"<http://lsdis.cs.uga.edu/projects/semdis/opus#Article>\")) "
            + "OR (predicate IN (\"<http://lsdis.cs.uga.edu/projects/semdis/opus#at_organization>\",\"<http://lsdis.cs.uga.edu/projects/semdis/opus#pages>\"));";
    
    
    private static final String Q_PART_2_B = " where (object=306863398384 "
            + "and predicate=0) OR (predicate IN (1200228370848,370280483312));";
    
    
    private static final String Q_PART_2_C = " where (object IN (352848615584,306863398384) and predicate=0) "
            + "OR (predicate IN (1200228370848,370280483312));";
    
    
    private static final String Q_PART_2_D = " where (object=306863398384 and predicate=0);";
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }
    
    
    /**
     * Test that QG will use = rather than IN when only one item in the list   
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void testQueryGeneratorOneObject() throws FileNotFoundException, IOException {
        
        String table = "my-table-1";
                
        QueryGenerator<String> generator = this.getQueryGenerator("/schema.nt", table, false, TermUtils.RDFS_TBOX);
        
        List<String> queries = generator.getQueries();
        
        
        String expQuery = Q_PART_1 + table + Q_PART_2;
        
        validateQuery(queries, expQuery); 
        
    }
    
    /**
     * Test that QG will use IN operator when more than one item
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void testQueryGeneratorMultiObject() throws FileNotFoundException, IOException {
        
        String table = "my-table-2";
                
        QueryGenerator<String> generator = this.getQueryGenerator("/schema1.nt", table, false, TermUtils.RDFS_TBOX);
        
        List<String> queries = generator.getQueries();
        
        
        String expQuery = Q_PART_1 + table + Q_PART_2_A;
        
        validateQuery(queries, expQuery); 
        
    }
    
    /**
     * Test that QG will use = rather than IN when only one item in the list   
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void testEQueryGeneratorOneObject() throws FileNotFoundException, IOException {
        
        String table = "my-table-3";
                
        QueryGenerator<Long> generator = this.getQueryGenerator("/schema.csv", table, true, TermUtils.RDFS_TBOX);
        
        List<String> queries = generator.getQueries();
        
        
        String expQuery = Q_PART_1 + table + Q_PART_2_B;
        
        validateQuery(queries, expQuery); 
        
    }
    
    /**
     * //select subject, predicate from lubm_tables.lubm8n where (predicate=0 and object IN (826230032144,825156026960,1719301948992));
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void testEQueryGeneratorRDFTypeSchemaTriples() throws FileNotFoundException, IOException {
        
        String table = "my-table-3";
                
        QueryGenerator<Long> generator = this.getQueryGenerator("/schema.csv", table, true, Sets.newHashSet(SchemaURIType.RDFS_SUBCLASS));
        
        List<String> queries = generator.getQueries();
        
        
        String expQuery = Q_PART_1 + table + Q_PART_2_D;
        
        validateQuery(queries, expQuery); 
        
    }
    
    /**
     * Test that QG will use IN operator when more than one item
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void testEQueryGeneratorMultiObject() throws FileNotFoundException, IOException {
        
        String table = "my-table-4";
                
        QueryGenerator<Long> generator = this.getQueryGenerator("/schema1.csv", table, true, TermUtils.RDFS_TBOX);
        
        List<String> queries = generator.getQueries();
        
        
        String expQuery = Q_PART_1_A + table + Q_PART_2_C;
        
        validateQuery(queries, expQuery); 
        
    }
    
    private void validateQuery(List<String> queries, String expQuery) {
        System.out.println(queries);
        
        assertEquals(1, queries.size());
        
        String query = queries.get(0);
        
        assertEquals(expQuery, query); 
    }

    /**
     * 
     * @param schemaFile
     * @return
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    private QueryGenerator getQueryGenerator(String schemaFile, String table, boolean encoded, 
            Set<SchemaURIType> predicates) throws FileNotFoundException, IOException {

        QueryGenerator generator = null;

        URL schemaUrl = QueryGeneratorTest.class.getResource(schemaFile);

        if(encoded) {

            Map<Long, Set<Triple>> schemaTriples = 
                    TripleUtils.getRelevantSchemaETriples(schemaUrl.getPath(), predicates);

           /* Map<Long, Set<Triple>> schemaTerms = new HashMap<>();
            for(Long term: schemaTriples.keySet()) {
                schemaTerms.put(term, schemaTriples.get(term));
            }*/

            generator = new QueryGenerator<Long>(schemaTriples, table);

        } else {

            Map<String, Set<Triple>> schemaTriples = 
                    TripleUtils.getRelevantSchemaNTriples(schemaUrl.getPath(), predicates);

            /*Map<String, Set<Triple>> schemaTerms = new HashMap<>();
            for(String term: schemaTriples.keySet()) {
                schemaTerms.put(term, schemaTriples.get(term));
            }*/

            generator = new QueryGenerator<String>(schemaTriples, table);
        }

        return generator;
    }
}
