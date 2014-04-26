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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

}
