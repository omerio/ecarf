/**
 * 
 */
package io.ecarf.core.triple;

import io.ecarf.core.utils.Constants;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.mortbay.log.Log;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.util.NxUtil;

/**
 * @author omerio
 *
 */
public class TripleUtils {


	/**
	 * Analyse the provided schema file and return a set containing all the relevant triples keyed by their terms
	 * @param schemaFile
	 * @param relevantUris
	 * @return
	 * TODO enhance so that we can selectively add the subject or the object of the schema triple 
	 * or both
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static Map<String, Set<Triple>> getRelevantSchemaNTriples(String schemaFile, Set<SchemaURIType> relevantUris) 
			throws FileNotFoundException, IOException {

		Map<String, Set<Triple>> schemaTriples = new HashMap<>();
		Triple triple;
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
					String object = terms[2];

					if(SchemaURIType.isSchemaUri(predicate) && 
							SchemaURIType.isRdfTbox(predicate) && 
							relevantUris.contains(SchemaURIType.getByUri(predicate))) {

						triple = new NTriple(subject, predicate, object);

						// subject is used for ABox (instance) reasoning
						if(!schemaTriples.containsKey(subject)) {
							schemaTriples.put(subject, new HashSet<Triple>());
						}
						schemaTriples.get(subject).add(triple);
					}

				} else {
					Log.warn("Ignoring line: " + ns);
				}
			}

		}

		return schemaTriples;
	}
	
	/**
	 * 
	 * @param schemaFile
	 * @param relevantUris
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static Map<Long, Set<Triple>> getRelevantSchemaETriples(String schemaFile, Set<SchemaURIType> relevantUris) 
	        throws FileNotFoundException, IOException {

	    Map<Long, Set<Triple>> schemaTriples = new HashMap<>();
	    ETriple triple;

	    try(Reader reader = new InputStreamReader(new BufferedInputStream(
	            new FileInputStream(schemaFile), Constants.GZIP_BUF_SIZE), Constants.UTF8)) {

	        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);


	        for (CSVRecord record : records) {

	            String [] terms = record.values();

	            /*for (int i = 0; i < ns.length; i++)  {
                    terms[i] = NxUtil.unescape(ns[i].toN3());
                }*/

	            // subject = terms[0];
	            String predicate = terms[1];
	            //String object = terms[2];
	            SchemaURIType type = SchemaURIType.getById(predicate);

	            if((type != null) && type.isRdf() && type.isTbox() && relevantUris.contains(type)) {

	                triple = ETriple.fromCSV(terms);

	                Long subject = triple.getSubject();

	                // subject is used for ABox (instance) reasoning
	                if(!schemaTriples.containsKey(subject)) {
	                    schemaTriples.put(subject, new HashSet<Triple>());
	                }
	                schemaTriples.get(subject).add(triple);
	            }

	        }

	    }

	    return schemaTriples;

	}
	
	/**
	 * Convert a csv file to triples
	 * @param triplesFile
	 * @param encoded
	 * @return
	 * @throws IOException
	 */
	public static List<Triple> csvToTriples(String triplesFile, boolean encoded) throws IOException {
	    
	    List<Triple> triples = new ArrayList<>();
	    
	    try (BufferedReader r = new BufferedReader(new FileReader(triplesFile), Constants.GZIP_BUF_SIZE)) {

	        triples.addAll(csvToTriples(r, encoded));
	    }
	    
	    return triples;
	}
	
	/**
	 * Get triples from a reader
	 * @param reader
	 * @param encoded
	 * @return
	 * @throws IOException
	 */
	public static List<Triple> csvToTriples(Reader reader, boolean encoded) throws IOException {
	    
	    List<Triple> triples = new ArrayList<>();
	    
	    Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);

        for (CSVRecord record : records) {

            if(encoded) {
                triples.add(ETriple.fromCSV(record.values()));
                
            } else {
                triples.add(NTriple.fromCSV(record.values()));
            }
        }
        
        return triples;
	}
	
	/**
	 * Populate the provided collection with the loaded triples
	 * @param triplesFile
	 * @param triples
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void loadNTriples(String triplesFile, Collection<Triple> triples) throws FileNotFoundException, IOException {
	    try (BufferedReader r = new BufferedReader(new FileReader(triplesFile))) {

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
                    String object = terms[2];

                    triples.add(new NTriple(subject, predicate, object));

                } else {
                    Log.warn("Ignoring line: " + ns);
                }
            }

        }
	   
	}

	/**
	 * Load N triples from a file
	 * @param triplesFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static Set<Triple> loadNTriples(String triplesFile) throws FileNotFoundException, IOException {

		Set<Triple> triples = new HashSet<>();

		loadNTriples(triplesFile, triples);

		return triples;
	}
	
	/**
     * load CSV triples from a file
     * @param triplesFile
     * @return
     */
    public static Set<Triple> loadCompressedCSVTriples(String triplesFile, boolean encoded)  throws FileNotFoundException, IOException  {
        return loadCompressedCSVTriples(triplesFile, encoded, null);
    }
	
	/**
	 * load CSV triples from a file
	 * @param triplesFile
	 * @return
	 */
	public static Set<Triple> loadCompressedCSVTriples(String triplesFile, boolean encoded, Set<Triple> triples)  throws FileNotFoundException, IOException  {
	    
	    if(triples == null) {
	        triples = new HashSet<>();
	    }
	    
		try(Reader reader = new InputStreamReader(new GZIPInputStream(
				new FileInputStream(triplesFile), Constants.GZIP_BUF_SIZE), Constants.UTF8)) {
			
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);

			if(encoded) {
			    for (CSVRecord record : records) {
                    triples.add(ETriple.fromCSV(record.values()));
                }
			    
			} else {
			    
			    for (CSVRecord record : records) {
			        triples.add(NTriple.fromCSV(record.values()));
			    }
			}

		}
		
		return triples;
		
	}
	
	
	// -------------------------------------------- NOT USED --------------------------------------------

	/**
	 * 
	 * @param triples
	 * @return
	 */
	public static Map<String, Set<Triple>> createTripleStore(Set<Triple> triples) {
		Map<String, Set<Triple>> store = new HashMap<>();
		addToTriples(triples, store);
		return store;
	}

	/**
	 * 
	 * @param triples
	 * @param store
	 */
	public static void addToTriples(Set<Triple> triples, Map<String, Set<Triple>> store) {
		for(Triple triple: triples) {
			addToTriples(triple.getSubject().toString(), triple, store);
			addToTriples(triple.getPredicate().toString(), triple, store);
			addToTriples(triple.getObject().toString(), triple, store);
		}
	}

	/**
	 * 
	 * @param term
	 * @param triple
	 * @param store
	 */
	public static void addToTriples(String term, Triple triple, Map<String, Set<Triple>> store) {
		if(store.get(term) == null) {
			store.put(term, new HashSet<Triple>());
		}
		store.get(term).add(triple);
	}

	/**
	 * 
	 * @param term
	 * @param triples
	 * @param store
	 */
	public static void addToTriples(String term, Set<Triple> triples, Map<String, Set<Triple>> store) {
		if(store.get(term) == null) {
			store.put(term, new HashSet<Triple>());
		}
		store.get(term).addAll(triples);
	}

	/**
	 * 
	 * @param triples1
	 * @param triple
	 * @param triples2
	 */
	public static void merge(Map<String, Set<Triple>> triples1, Map<String, Set<Triple>> triples2) {
		for(Entry<String, Set<Triple>> entry: triples1.entrySet()) {
			addToTriples(entry.getKey(), entry.getValue(), triples2);
		}
	}

}
