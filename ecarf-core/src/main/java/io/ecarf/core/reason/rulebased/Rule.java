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

import io.ecarf.core.triple.Triple;

import java.util.List;
import java.util.Map;

/**
 * For now the assumption is the rule will contain one schema triple
 * and one instance triple. This will change once all of the OWL 2 RL rule 
 * are implemented
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface Rule {
	
	/**
	 * Return a SQL like query that can be used to retrieve all the instance
	 * triples that forms the second part of the body of this rule. Like for example
	 * 
	 * select subject from swetodblp.swetodblp_triple where 
				 object = "<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>";
	 * @param schemaTriple
	 * @param instanceTriple
	 * @return
	 */
	public String query(Triple schemaTriple, String table);
	
	/**
	 * Return a SQL like query that can be used to retrieve all the instance
	 * triples that forms the second part of the body of this rule. Like for example
	 * 
	 * select subject from swetodblp.swetodblp_triple where 
				 object = "<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>";
	 * @param schemaTriple
	 * @param instanceTriple
	 * @param table
	 * @param select - custom select columns to use
	 * @return
	 */
	public String query(Triple schemaTriple, String table, List<String> select);

	/**
	 * return the select column
	 * @return
	 */
	public List<String> select();

	/**
	 * return the items to use in the select clause
	 * @param schemaTriple
	 * @return
	 */
	public Map<String, String> where(Triple schemaTriple);

	/**
	 * Generate a triple if this rules fires in the format T(?x, rdf:type, ?c)
	 * @param schemaTriple
	 * @param instanceTriple
	 * @return
	 */
	public Triple head(Triple schemaTriple, Triple instanceTriple);

}
