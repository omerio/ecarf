/**
 * 
 */
package io.ecarf.core.triple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author omerio
 *
 */
public class TripleUtils {

	/**
	 * Parse an N Triple, based on a utility from Webpie 
	 * (http://www.few.vu.nl/~jui200/webpie.html)
	 * FIXME rewrite or use a triple parsing library (Jena, etc...)
	 * @param triple
	 * @return
	 * @throws Exception
	 */
	public static String[] parseTriple(String triple) {
		String[] values = null;

		// added by Omer, ignore comments
		if(!triple.startsWith("#")) {
			values = new String[3];

			// Parse subject
			if (triple.startsWith("<")) {
				values[0] = triple.substring(0, triple.indexOf('>') + 1);
			} else { // Is a bnode
				values[0] = triple.substring(0, triple.indexOf(' '));
			}

			triple = triple.substring(values[0].length() + 1);
			// Parse predicate. It can be only a URI
			values[1] = triple.substring(0, triple.indexOf('>') + 1);

			// Parse object
			triple = triple.substring(values[1].length() + 1);
			if (triple.startsWith("<")) { // URI
				values[2] = triple.substring(0, triple.indexOf('>') + 1);
			} else if (triple.charAt(0) == '"') { // Literal
				values[2] = triple.substring(0,
						triple.substring(1).indexOf('"') + 2);
				triple = triple.substring(values[2].length(), triple.length());
				values[2] += triple.substring(0, triple.indexOf(' '));
			} else { // Bnode
				values[2] = triple.substring(0, triple.indexOf(' '));
			}
		}

		return values;
	}
	
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
			addToTriples(triple.getSubject(), triple, store);
			addToTriples(triple.getPredicate(), triple, store);
			addToTriples(triple.getObject(), triple, store);
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
