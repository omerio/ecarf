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
package io.ecarf.core.term;

import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.mortbay.log.Log;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.util.NxUtil;

import com.google.common.collect.Sets;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TermUtils {
    
    public static final String HTTP = "http://";
    public static final String HTTPS = "https://";
    public static final char URI_SEP = '/';
    

	/**
	 * All the schema terms we care about in this version of the implementation
	 */
	public static final Set<SchemaURIType> RDFS_TBOX = Sets.newHashSet(
			SchemaURIType.RDFS_DOMAIN, 
			SchemaURIType.RDFS_RANGE, 
			SchemaURIType.RDFS_SUBCLASS, 
			SchemaURIType.RDFS_SUBPROPERTY);

	/**
	 * Analyse the provided schema file and return a set containing all the relevant terms
	 * @param schemaFile
	 * @param relevantUris
	 * @return
	 * TODO enhance so that we can selectively add the subject or the object of the schema triple 
	 * or both
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static Set<String> getRelevantSchemaTerms(String schemaFile, Set<SchemaURIType> relevantUris) 
			throws FileNotFoundException, IOException {

		Set<String> relevantTerms = new HashSet<String>();

		try (BufferedReader r = new BufferedReader(new FileReader(schemaFile))) {

			String[] terms;
			NxParser nxp = new NxParser(r);

			while (nxp.hasNext())  {

				Node[] ns = nxp.next();

				//We are only interested in triples, no quads
				if (ns.length == 3) {
					terms = new String [3];

					for (int i = 0; i < ns.length; i++)  {
						terms[i] = NxUtil.unescape(ns[i].toN3());
					}

					String subject = terms[0];
					String predicate = terms[1];
					//String object = terms[2];

					if(SchemaURIType.isSchemaUri(predicate) && 
							SchemaURIType.isRdfTbox(predicate) && 
							relevantUris.contains(SchemaURIType.getByUri(predicate))) {

						// subject is used for ABox (instance) reasoning
						relevantTerms.add(subject);
					}
				} else {
					Log.warn("Ignoring line: " + ns);
				}
			}
		}
		return relevantTerms;
	}
	
	/**
	 * Split a term into parts
	 * @param term
	 * @return
	 */
	public static List<String> split(String term) {
	    String url = term.substring(1, term.length() - 1);
        String path = StringUtils.removeStart(url, HTTP);
        
        
        if(path.length() == url.length()) {
            path = StringUtils.removeStart(path, HTTPS);
        }
        
        //String [] parts = StringUtils.split(path, URI_SEP);
        // this is alot faster than String.split or StringUtils.split
        return Utils.split(path, URI_SEP);
	}
	
	/**
	 * Check if the provided term is an RDF or OWL term
	 * @param term
	 * @return
	 */
	/*public static boolean isRdfOrOwlTerm(String term) {
	    boolean rdfOrOwlTerm = false;
	    
	    for(String rdfOwl: SchemaURIType.RDF_OWL_TERMS) {
	        if(TermUtils.equals(rdfOwl, term)) {
	            rdfOrOwlTerm = true;
	            break;
	        }
	    }
	    return rdfOrOwlTerm;
	}*/
	
	/**
	 * From NXParser 
     * Compares strings backwards... why? Cos it should be faster for URLs...
     * 
     * @param a
     *            A string
     * @param b
     *            A string
     * @return a = b ?
     */
    public static boolean equals(String a, String b) {
        if (a == b) {
            return true;
        }

        int n = a.length();
        if (n == b.length()) {
            while (--n >= 0) {
                // faster cos it reads directly from the array
                if (a.charAt(n) != b.charAt(n))
                    return false;
            }
            return true;
        }
        return false;
    }
    
    /*public static void main(String[] args) {
        Set<String> terms = Sets.newHashSet("<http://dblp.uni-trier.de/rec/bibtex/books/mk/WidomC96>",
                "<http://lsdis.cs.uga.edu/projects/semdis/opus#cites>", 
                "<http://dblp.uni-trier.de/rec/bibtex/conf/vldb/AgrawalCL91>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
        
        
        Stopwatch stopwatch = Stopwatch.createStarted();
        for(int i = 0; i < 10_000_000; i++) {
            for(String term: terms) {
                isRdfOrOwlTerm(term);
            }
        }
        
        System.out.println("Approach1 time: " + stopwatch);
        stopwatch.reset();
        stopwatch.start();
        
        for(int i = 0; i < 10_000_000; i++) {
            for(String term: terms) {
                SchemaURIType.RDF_OWL_TERMS.contains(term);
            }
        }
        
        System.out.println("Approach2 time: " + stopwatch);
        
    }
*/
    // public static void main(String args[]){
    // System.err.println(equals("asd", "asd"));
    // System.err.println(equals("asd", "asdf"));
    // System.err.println(equals("bsd", "asd"));
    //
    // long b4 = System.currentTimeMillis();
    // for(int i=0; i<10000000; i++){
    // "http://google.com/asd/asd".equals("http://google.com/asd/asd");
    // "http://google.com/asd/asd".equals("http://google.com/asd/asdf");
    // "http://google.com/asd/bsd".equals("http://google.com/asd/csd");
    // "http://google.com/asd/csd".equals("http://google.com/asd/dsd");
    // }
    //
    // System.err.println(System.currentTimeMillis()-b4);
    // b4 = System.currentTimeMillis();
    // for(int i=0; i<10000000; i++){
    // equals("http://google.com/asd/asd", "http://google.com/asd/asd");
    // equals("http://google.com/asd/asd", "http://google.com/asd/asdf");
    // equals("http://google.com/asd/bsd", "http://google.com/asd/csd");
    // equals("http://google.com/asd/csd", "http://google.com/asd/dsd");
    // }
    //
    // System.err.println(System.currentTimeMillis()-b4);
    // }

}
