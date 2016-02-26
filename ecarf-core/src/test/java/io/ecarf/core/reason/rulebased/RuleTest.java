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
package io.ecarf.core.reason.rulebased;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import io.ecarf.core.reason.rulebased.owl2rl.rdfs.CaxScoRule;
import io.ecarf.core.reason.rulebased.owl2rl.rdfs.PrpDomRule;
import io.ecarf.core.reason.rulebased.owl2rl.rdfs.PrpRngRule;
import io.ecarf.core.reason.rulebased.owl2rl.rdfs.PrpSpo1Rule;
import io.ecarf.core.triple.ETriple;
import io.ecarf.core.triple.NTriple;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.triple.Triple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;

import com.google.api.client.repackaged.com.google.common.base.Joiner;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class RuleTest {

    private static final String TERM1 = "<http://lsdis.cs.uga.edu/projects/semdis/opus#Article>";
    private static final Long   TERM1_1 = 306863398384L;
    
    private static final String TERM2 = "<http://dblp.uni-trier.de/rec/bibtex/journals/ijprai/Singh99>";
    private static final Long   TERM2_1 = 14912065592728L;
    
    private static final String TERM3 = "<http://xmlns.com/foaf/0.1/Document>";
    private static final Long   TERM3_1 = 7653280640864L;
    
    private static final String TERM4 = "<http://lsdis.cs.uga.edu/projects/semdis/opus#at_organization>";
    private static final Long   TERM4_1 = 1200228370848L;
    
    private static final String TERM5 = "<http://xmlns.com/foaf/0.1/Organization>";
    private static final Long   TERM5_1 = 74632293448224L;
    
    private static final String TERM6 = "<http://lsdis.cs.uga.edu/projects/semdis/opus#Publication>";
    private static final Long   TERM6_1 = 71752386272432L;
    
    private static final String TERM7 = "<http://dblp.uni-trier.de/rec/bibtex/ms/Yurek97>";
    private static final Long   TERM7_1 = 1514437763392L;
    
    private static final String TERM8 = "<http://www.ucsb.edu/>";
    private static final Long   TERM8_1 = 2636789L;
    
    private static final String TERM9 = "<http://lsdis.cs.uga.edu/projects/semdis/opus#pages>";
    private static final Long   TERM9_1 = 370280483312L;
    //<http://lsdis.cs.uga.edu/projects/semdis/opus#pages> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.w3.org/2002/07/owl#topDataProperty> .
   
    @Test
    public void testEPrpSpo1Rule() {
        Triple schemaTriple = getETriple(TERM9_1, (long) SchemaURIType.RDFS_SUBPROPERTY.id,(long) SchemaURIType.OWL_TOP_DATA_PROPERTY.id, null);
        Triple instanceTriple = getETriple(TERM2_1, TERM9_1, null, "42-68");
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof PrpSpo1Rule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNotNull(infTriple);
        
        assertEquals(getETriple(TERM2_1, (long) SchemaURIType.OWL_TOP_DATA_PROPERTY.id, null, "42-68"), infTriple);
    }
    
  
    @Test
    public void testEPrpRngRuleLiteral() {
        Triple schemaTriple = getETriple(TERM9_1, (long) SchemaURIType.RDFS_RANGE.id, (long) SchemaURIType.XML_STRING.id, null);
        Triple instanceTriple = getETriple(TERM2_1, TERM9_1, null, "42-68");
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof PrpRngRule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNull(infTriple);
        
        //assertEquals(getNTriple(TERM8, SchemaURIType.RDF_TYPE.uri, TERM5), infTriple);
    }
    
    @Test
    public void testEPrpRngRule() {
        Triple schemaTriple = getETriple(TERM4_1, (long) SchemaURIType.RDFS_RANGE.id, TERM5_1, null);
        Triple instanceTriple = getETriple(TERM7_1, TERM4_1, TERM8_1, null);
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof PrpRngRule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNotNull(infTriple);
        
        assertEquals(getETriple(TERM8_1, (long) SchemaURIType.RDF_TYPE.id, TERM5_1, null), infTriple);
    }
    
    
    
    @Test
    public void testECaxScoRule() {
        Triple schemaTriple = getETriple(TERM1_1, (long) SchemaURIType.RDFS_SUBCLASS.id, TERM3_1, null);
        Triple instanceTriple = getETriple(TERM2_1, (long) SchemaURIType.RDF_TYPE.id, TERM1_1, null);
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof CaxScoRule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNotNull(infTriple);
        
        assertEquals(getETriple(TERM2_1, (long) SchemaURIType.RDF_TYPE.id, TERM3_1, null), infTriple);
    }
    
    
    @Test
    public void testEPrpDomRule() {
        Triple schemaTriple = getETriple(TERM4_1, (long) SchemaURIType.RDFS_DOMAIN.id, TERM6_1, null);
        Triple instanceTriple = getETriple(TERM7_1, TERM4_1, TERM8_1, null);
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof PrpDomRule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNotNull(infTriple);
        
        assertEquals(getETriple(TERM7_1, (long) SchemaURIType.RDF_TYPE.id, TERM6_1, null), infTriple);
    }
    
    @Test
    public void testEMultiRules() {
        Long term = 17893680928928L; //"<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter>"
        Map<Long, Set<Triple>> schemaTriples = new HashMap<>();
        Set<Triple> triples = new HashSet<Triple>();

        triples.add(getETriple(term, (long) SchemaURIType.RDFS_DOMAIN.id, 306929376416L, null));
        triples.add(getETriple(term, (long) SchemaURIType.RDFS_SUBPROPERTY.id, (long) SchemaURIType.OWL_TOP_DATA_PROPERTY.id, null));
        triples.add(getETriple(term, (long) SchemaURIType.RDFS_RANGE.id, (long) SchemaURIType.XML_STRING.id, null));
        schemaTriples.put(term, triples);

        Set<Triple> instanceTriples = new HashSet<>();
        instanceTriples.add(getETriple(888888888888L, term, null, "\"21\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
        instanceTriples.add(getETriple(666666666666L, term, null, "\"21\"^^<http://www.w3.org/2001/XMLSchema#integer>"));

        Set<Triple> inferredTriples = new HashSet<>();

        for(Entry<Long, Set<Triple>> entry: schemaTriples.entrySet()) {

            term = entry.getKey();
            System.out.println("Reasoning for term: " + term);
            triples = entry.getValue();

            // loop through the instance triples probably stored in a file and generate all the triples matching the schema triples set
            for(Triple instanceTriple: instanceTriples) {   
                for(Triple schemaTriple: triples) {
                    Rule rule = GenericRule.getRule(schemaTriple);
                    Triple infTriple = rule.head(schemaTriple, instanceTriple);
                    if(infTriple != null) {
                        inferredTriples.add(infTriple);
                    }
                }
            }
        }

        // expecting 4 triples, 2 of them will have a literal in the subject place and won't be generated
        assertEquals(4, inferredTriples.size());
        System.out.println("\nInferred Triples");
        System.out.println(Joiner.on('\n').join(inferredTriples));

    }
    
    //------------------------------------------------------------- NTriples
    @Test
    public void testPrpSpo1Rule() {
        Triple schemaTriple = getNTriple(TERM9, SchemaURIType.RDFS_SUBPROPERTY.uri, SchemaURIType.OWL_TOP_DATA_PROPERTY.uri);
        Triple instanceTriple = getNTriple(TERM2, TERM9, "42-68");
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof PrpSpo1Rule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNotNull(infTriple);
        
        assertEquals(getNTriple(TERM2, SchemaURIType.OWL_TOP_DATA_PROPERTY.uri, "42-68"), infTriple);
    }
    
  
    @Test
    public void testPrpRngRuleLiteral() {
        Triple schemaTriple = getNTriple(TERM9, SchemaURIType.RDFS_RANGE.uri, SchemaURIType.XML_STRING.uri);
        Triple instanceTriple = getNTriple(TERM2, TERM9, "42-68");
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof PrpRngRule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNull(infTriple);
        
        //assertEquals(getNTriple(TERM8, SchemaURIType.RDF_TYPE.uri, TERM5), infTriple);
    }
    
    @Test
    public void testPrpRngRule() {
        Triple schemaTriple = getNTriple(TERM4, SchemaURIType.RDFS_RANGE.uri, TERM5);
        Triple instanceTriple = getNTriple(TERM7, TERM4, TERM8);
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof PrpRngRule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNotNull(infTriple);
        
        assertEquals(getNTriple(TERM8, SchemaURIType.RDF_TYPE.uri, TERM5), infTriple);
    }
    
    
    
    @Test
    public void testCaxScoRule() {
        Triple schemaTriple = getNTriple(TERM1, SchemaURIType.RDFS_SUBCLASS.uri, TERM3);
        Triple instanceTriple = getNTriple(TERM2, SchemaURIType.RDF_TYPE.uri, TERM1);
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof CaxScoRule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNotNull(infTriple);
        
        assertEquals(getNTriple(TERM2, SchemaURIType.RDF_TYPE.uri, TERM3), infTriple);
    }
    
    
    @Test
    public void testPrpDomRule() {
        Triple schemaTriple = getNTriple(TERM4, SchemaURIType.RDFS_DOMAIN.uri, TERM6);
        Triple instanceTriple = getNTriple(TERM7, TERM4, TERM8);
        
        Rule rule = GenericRule.getRule(schemaTriple);
        
        assertTrue(rule instanceof PrpDomRule);
        
        Triple infTriple = rule.head(schemaTriple, instanceTriple);
        
        assertNotNull(infTriple);
        
        assertEquals(getNTriple(TERM7, SchemaURIType.RDF_TYPE.uri, TERM6), infTriple);
    }
    
    
    
    /**
     * 
     * @param subject
     * @param predicate
     * @param object
     * @param objectLiteral
     * @param encoded
     * @return
     */
    private Triple getNTriple(String subject, String predicate, String object) {
        Triple triple = new NTriple( subject, (String) predicate, (String) object);        
        return triple;
    }
    
    private Triple getETriple(Long subject, Long predicate, Long object, String objectLiteral) {
        Triple triple = new ETriple(subject, predicate, object, objectLiteral);
        return triple;
    }

    @Test
    public void testMultiRules() {
        String term = "<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter>";
        Map<String, Set<Triple>> schemaTriples = new HashMap<>();
        Set<Triple> triples = new HashSet<Triple>();
        /*
         * 
	        Triple [<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter> <http://www.w3.org/2000/01/rdf-schema#domain> <http://lsdis.cs.uga.edu/projects/semdis/opus#Book_Chapter>], inferred=false
	        Triple [<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.w3.org/2002/07/owl#topDataProperty>], inferred=false
	        Triple [<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2001/XMLSchema#string>], inferred=false
         */
        triples.add(getNTriple(term, "<http://www.w3.org/2000/01/rdf-schema#domain>", "<http://lsdis.cs.uga.edu/projects/semdis/opus#Book_Chapter>"));
        triples.add(getNTriple(term, "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>", "<http://www.w3.org/2002/07/owl#topDataProperty>"));
        triples.add(getNTriple(term, "<http://www.w3.org/2000/01/rdf-schema#range>", "<http://www.w3.org/2001/XMLSchema#string>"));
        schemaTriples.put(term, triples);

        Set<Triple> instanceTriples = new HashSet<>();
        instanceTriples.add(getNTriple("<http://dblp.uni-trier.de/rec/bibtex/books/kl/snodgrass95/KlineSL95>", term, "\"21\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
        instanceTriples.add(getNTriple("<http://dblp.uni-trier.de/rec/bibtex/books/kl/snodgrass95/SooJS95>", term, "\"27\"^^<http://www.w3.org/2001/XMLSchema#integer>"));

        Set<Triple> inferredTriples = new HashSet<>();

        for(Entry<String, Set<Triple>> entry: schemaTriples.entrySet()) {

            term = entry.getKey();
            System.out.println("Reasoning for term: " + term);
            triples = entry.getValue();

            // loop through the instance triples probably stored in a file and generate all the triples matching the schema triples set
            for(Triple instanceTriple: instanceTriples) {   
                for(Triple schemaTriple: triples) {
                    Rule rule = GenericRule.getRule(schemaTriple);
                    Triple infTriple = rule.head(schemaTriple, instanceTriple);
                    if(infTriple != null) {
                        inferredTriples.add(infTriple);
                    }
                }
            }
        }

        // expecting 4 triples, 2 of them will have a literal in the subject place and won't be generated
        assertEquals(4, inferredTriples.size());
        System.out.println("\nInferred Triples");
        System.out.println(Joiner.on('\n').join(inferredTriples));

    }

    /**
     * Test method for {@link io.ecarf.core.reason.rulebased.GenericRule#query(io.ecarf.core.triple.Triple, io.ecarf.core.triple.Triple, java.lang.String)}.
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    //@Test
    /*public void testQueryTripleTriple() throws FileNotFoundException, IOException {
		Map<String, Set<Triple>> schemaTriples = 
				TripleUtils.getRelevantSchemaNTriples(
						//"/Users/omerio/Ontologies/dbpedia/tbox/dbpedia_3.9_gen_closure.nt",
						"/Users/omerio/Ontologies/opus_august2007_closure.nt", 
						TermUtils.RDFS_TBOX);
		for(Entry<String, Set<Triple>> entry: schemaTriples.entrySet()) {
			System.out.println("Term: " + entry.getKey());
			System.out.println("Triples: " + Joiner.on('\n').join(entry.getValue()));
			List<String> select = GenericRule.getSelect(entry.getValue());
			System.out.println("\nQuery: " + GenericRule.getQuery(entry.getValue(), "my-table"));
			System.out.println("Select: " + select);
			System.out.println("------------------------------------------------------------------------------------------------------------");
		}
	}


	//@Test
	public void testQueryETripleTriple() throws FileNotFoundException, IOException {
	    Map<Long, Set<Triple>> schemaTriples = 
	            TripleUtils.getRelevantSchemaETriples(
	                    //"/Users/omerio/Ontologies/dbpedia/tbox/dbpedia_3.9_gen_closure.nt",
	                    "/Users/omerio/Downloads/opus_august2007_closure_encoded.csv", 
	                    TermUtils.RDFS_TBOX);
	    for(Entry<Long, Set<Triple>> entry: schemaTriples.entrySet()) {
	        System.out.println("Term: " + entry.getKey());
	        System.out.println("Triples: " + Joiner.on('\n').join(entry.getValue()));
	        List<String> select = GenericRule.getSelect(entry.getValue());
	        System.out.println("\nQuery: " + GenericRule.getQuery(entry.getValue(), "my-table"));
	        System.out.println("Select: " + select);
	        System.out.println("------------------------------------------------------------------------------------------------------------");
	    }
	}*/

    /*	
	@Test
	public void testQueryGenerator() throws FileNotFoundException, IOException {
		Map<String, Set<Triple>> schemaTriples = 
				TripleUtils.getRelevantSchemaNTriples(
						//"/Users/omerio/Ontologies/dbpedia/tbox/dbpedia_3.9_gen_closure.nt",
						//"/Users/omerio/Ontologies/opus_august2007_closure.nt",
				        "/Users/omerio/Ontologies/opus_august2007_closure_owl_api.nt",
						TermUtils.RDFS_TBOX);

		Map<String, Set<Triple>> schemaTerms = new HashMap<>();
		for(String term: schemaTriples.keySet()) {
			schemaTerms.put(term, schemaTriples.get(term));
		}

		QueryGenerator<String> generator = new QueryGenerator<String>(schemaTerms, "table");
		System.out.println(generator.getQueries());
	}

	@Test
    public void testEQueryGenerator() throws FileNotFoundException, IOException {
        Map<Long, Set<Triple>> schemaTriples = 
                TripleUtils.getRelevantSchemaETriples(
                        "/Users/omerio/Downloads/opus_august2007_closure_encoded.csv",
                        //"/Users/omerio/Ontologies/opus_august2007_closure.nt", 
                        TermUtils.RDFS_TBOX);

        Map<Long, Set<Triple>> schemaTerms = new HashMap<>();
        for(Long term: schemaTriples.keySet()) {
            schemaTerms.put(term, schemaTriples.get(term));
        }

        QueryGenerator<Long> generator = new QueryGenerator<Long>(schemaTerms, "table");
        System.out.println(generator.getQueries());
    }*/


}
