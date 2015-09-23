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
import java.util.ArrayList;
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
    public static final String URI_SEP_STR = "/";
    

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
	
	private static int getCharIdxBeforeOrAfterIdx(String text, int idx, char chr) {
        // try before
        int index = text.substring(0, idx).lastIndexOf(chr);
        if(index < 0) {
            // try after
            index = text.substring(idx, text.length()).indexOf(chr);
            if(index > -1) {
                index += idx;
            }
        }
        return index;
    }
	
	/**
	 * Split into two
	 * @param term
	 * @return
	 */
	public static List<String> splitIntoTwo(String term) {
	    return splitIntoTwo(term, true);
	}
    
	/**
	 * Split the provided term into 2 parts using the slash a separator. Uses some rules concerning : and ?
	 * Some examples:
	 * <http://patft.uspto.gov/netacgi/nph-Parser?Sect1=PTO1&Sect2=HITOFF&d=PALL&p=1&u=/netahtml/PTO/srchnum.htm&r=1&f=G&l=50&s1=6348648.PN.&OS=PN/6348648&RS=PN/6348648/>
        [patft.uspto.gov/netacgi, nph-Parser?Sect1=PTO1&Sect2=HITOFF&d=PALL&p=1&u=/netahtml/PTO/srchnum.htm&r=1&f=G&l=50&s1=6348648.PN.&OS=PN/6348648&RS=PN/6348648/]

        <http://www.honda.lv/>
        [www.honda.lv]
        
        <http://gmail.com>
        [gmail.com]
        
        <http://gmail.com:8080/Test?id=test>
        [gmail.com:8080, Test?id=test]
        
        <http://web.archive.org/web/20051031200142/http:/www.mkaz.com/ebeab/history/>
        [web.archive.org/web/20051031200142, http:/www.mkaz.com/ebeab/history/]
        
        <http://web.archive.org/web/20051031200142/?http:/www.mkaz.com/ebeab/history/>
        [web.archive.org/web/20051031200142, ?http:/www.mkaz.com/ebeab/history/]
        
        <http://web.archive.org/web/20051031200142/http:/www.mkaz.com?id=ebeab/history/>
        [web.archive.org/web/20051031200142, http:/www.mkaz.com?id=ebeab/history/]
        
        <http://www.hel.fi/wps/portal/Helsinki_en/?WCM_GLOBAL_CONTEXT=/en/Helsinki/>
        [www.hel.fi/wps/portal/Helsinki_en, ?WCM_GLOBAL_CONTEXT=/en/Helsinki/]
        
        <http://dbpedia.org/resource/Team_handball>
        [dbpedia.org/resource, Team_handball]
        
        <http://dbpedia.org/ontology/wikiPageExternalLink>
        [dbpedia.org/ontology, wikiPageExternalLink]
        
        <http://www.nfsa.gov.au/blog/2012/09/28/tasmanian-time-capsule/>
        [www.nfsa.gov.au/blog/2012/09/28/tasmanian-time-capsule]
        
        <http://www.whereis.com/whereis/mapping/renderMapAddress.do?name=&streetNumber=&street=City%20Center&streetType=&suburb=Hobart&state=Tasmania&latitude=-42.881&longitude=147.3265&navId=$01006046X0OL9$&brandId=1&advertiser
        Id=&requiredZoomLevel=3>
        [www.whereis.com/whereis/mapping, renderMapAddress.do?name=&streetNumber=&street=City%20Center&streetType=&suburb=Hobart&state=Tasmania&latitude=-42.881&longitude=147.3265&navId=$01006046X0OL9$&brandId=1&advertiserId=&re
        quiredZoomLevel=3]

	 * @param term
	 * @return
	 */
    public static List<String> splitIntoTwo(String term, boolean hasProtocol) {
        
        String path;
        
        if(hasProtocol) {
            String url = term.substring(1, term.length() - 1);
            path = StringUtils.removeStart(url, TermUtils.HTTP);

            if(path.length() == url.length()) {
                path = StringUtils.removeStart(path, TermUtils.HTTPS);
            }
            
        } else {
            path = term;
        }
        
        // remove trailing slash
        if(StringUtils.endsWith(path, URI_SEP_STR)) {
            path = StringUtils.removeEnd(path, URI_SEP_STR);
        }
        
        //System.out.println(path);
        List<String> parts = new ArrayList<>();
        
        int slashIdx = path.lastIndexOf(TermUtils.URI_SEP);
        int colonIdx = path.indexOf(':');
        int questionIdx = path.indexOf('?');
        
        if(((colonIdx > -1) && (slashIdx > colonIdx)) || ((questionIdx > -1) && (slashIdx > questionIdx))) {
            
            int idx = -1;
            
           /* if((colonIdx > -1) && (questionIdx > -1)) {
                if(colonIdx < questionIdx) {
                    
                    idx = getCharIdxBeforeOrAfterIdx(path, colonIdx, TermUtils.URI_SEP);
                    
                } else {
                    idx = getCharIdxBeforeOrAfterIdx(path, questionIdx, TermUtils.URI_SEP);
                }
                
            } else if(colonIdx > -1) {
                
                idx = getCharIdxBeforeOrAfterIdx(path, colonIdx, TermUtils.URI_SEP);
                
            } else if(questionIdx > -1) {
                
                idx = getCharIdxBeforeOrAfterIdx(path, questionIdx, TermUtils.URI_SEP);
            }*/
            
            boolean colonAndQuestion = (colonIdx > -1) && (questionIdx > -1);
            
            if((colonAndQuestion && (colonIdx < questionIdx)) || (colonIdx > -1)) {
                
                idx = getCharIdxBeforeOrAfterIdx(path, colonIdx, TermUtils.URI_SEP);
                
            } else if((colonAndQuestion && (colonIdx > questionIdx)) || (questionIdx > -1)) {
                
                idx = getCharIdxBeforeOrAfterIdx(path, questionIdx, TermUtils.URI_SEP);
            }
            
            if(idx > -1) {
                slashIdx = idx;
            } 
        }
        
        
        
        //System.out.println(slashIdx);
        
        if(slashIdx > -1) {
            String part = path.substring(0, slashIdx);
            if(part.length() > 0) {
                parts.add(part);
            }
            slashIdx++;
            
            if(slashIdx < path.length()) {
                parts.add(path.substring(slashIdx));
            }
            
        } else {
            parts.add(path);
        }
        
        
        
        
       /* int foundPosition;
        int startIndex = 0;
        String part;
        while ((foundPosition = path.indexOf(TermUtils.URI_SEP, startIndex)) > -1) {
            part = path.substring(startIndex, foundPosition);
            if(part.length() > 0) {
                parts.add(part);
            }
            startIndex = foundPosition + 1;
        }
        if(startIndex  < path.length()) {
            parts.add(path.substring(startIndex));
        }*/
        
        return parts;
    }
	
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
