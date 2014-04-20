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

import io.ecarf.core.term.TermUtils;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.api.client.repackaged.com.google.common.base.Joiner;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class RuleTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link io.ecarf.core.reason.rulebased.GenericRule#query(io.ecarf.core.triple.Triple, io.ecarf.core.triple.Triple, java.lang.String)}.
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void testQueryTripleTriple() throws FileNotFoundException, IOException {
		Map<String, Set<Triple>> schemaTriples = 
				TripleUtils.getRelevantSchemaTriples(
						//"/Users/omerio/Ontologies/dbpedia/tbox/dbpedia_3.9_gen_closure.nt",
						"/Users/omerio/Ontologies/opus_august2007_closure.nt", 
						TermUtils.RDFS_TBOX);
		for(Entry<String, Set<Triple>> entry: schemaTriples.entrySet()) {
			System.out.println("Term: " + entry.getKey());
			System.out.println("Triples: " + Joiner.on('\n').join(entry.getValue()));
			Set<String> select = GenericRule.getSelect(entry.getValue());
			System.out.println("\nQuery: " + GenericRule.getQuery(entry.getValue(), "my-table"));
			System.out.println("Select: " + select);
			System.out.println("------------------------------------------------------------------------------------------------------------");
		}
	}
	
	@Test
	public void testReason() {
		String term = "<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter>";
		Map<String, Set<Triple>> schemaTriples = new HashMap<>();
		Set<Triple> triples = new HashSet<Triple>();
		/*
		 * 
		Triple [<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter> <http://www.w3.org/2000/01/rdf-schema#domain> <http://lsdis.cs.uga.edu/projects/semdis/opus#Book_Chapter>], inferred=false
		Triple [<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.w3.org/2002/07/owl#topDataProperty>], inferred=false
		Triple [<http://lsdis.cs.uga.edu/projects/semdis/opus#chapter> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2001/XMLSchema#string>], inferred=false
		 */
		triples.add(new Triple(term, "<http://www.w3.org/2000/01/rdf-schema#domain>", "<http://lsdis.cs.uga.edu/projects/semdis/opus#Book_Chapter>"));
		triples.add(new Triple(term, "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>", "<http://www.w3.org/2002/07/owl#topDataProperty>"));
		triples.add(new Triple(term, "<http://www.w3.org/2000/01/rdf-schema#range>", "<http://www.w3.org/2001/XMLSchema#string>"));
		schemaTriples.put(term, triples);
		
		Set<Triple> instanceTriples = new HashSet<>();
		instanceTriples.add(new Triple("<http://dblp.uni-trier.de/rec/bibtex/books/kl/snodgrass95/KlineSL95>", term, "\"21\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
		instanceTriples.add(new Triple("<http://dblp.uni-trier.de/rec/bibtex/books/kl/snodgrass95/SooJS95>", term, "\"27\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
		
		Set<Triple> inferredTriples = new HashSet<>();
		
		for(Entry<String, Set<Triple>> entry: schemaTriples.entrySet()) {
			
			term = entry.getKey();
			System.out.println("Reasoning for term: " + term);
			triples = entry.getValue();
			
			// loop through the instance triples probably stored in a file and generate all the triples matching the schema triples set
			for(Triple instanceTriple: instanceTriples) {	
				for(Triple schemaTriple: triples) {
					Rule rule = GenericRule.getRule(schemaTriple);
					inferredTriples.add(rule.head(schemaTriple, instanceTriple));
				}
			}
		}
		System.out.println("\nInferred Triples");
		System.out.println(Joiner.on('\n').join(inferredTriples));
		
		
	}

}
