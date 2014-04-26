/**
 * 
 */
package io.ecarf.core.triple;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
	public static Map<String, Set<Triple>> getRelevantSchemaTriples(String schemaFile, Set<SchemaURIType> relevantUris) 
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

						triple = new Triple(subject, predicate, object);

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
