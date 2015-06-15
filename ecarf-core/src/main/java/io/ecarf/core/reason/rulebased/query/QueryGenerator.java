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

import io.ecarf.core.cloud.task.impl.reason.Term;
import io.ecarf.core.reason.rulebased.GenericRule;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Joiner;

/**
 * This class takes in schema terms and their triples and generates a combined query for all the rules.
 * The query is split according to size if it's too large
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class QueryGenerator {
	
	private final static Log log = LogFactory.getLog(QueryGenerator.class);
	
	private static final String SELECT_FROM = "select subject, predicate, object from ";
	
	private String decoratedTable;
	private Map<Term, Set<Triple>> schemaTerms; 

	/**
	 * 
	 */
	public QueryGenerator(Map<Term, Set<Triple>> schemaTerms, String decoratedTable) {
		this.decoratedTable = decoratedTable;
		this.schemaTerms = schemaTerms;
	}
	
	public List<String> getQueries() {
		List<String> queries = new ArrayList<String>();
		
		Map<Integer, Map<String, Set<String>>> parts = new HashMap<Integer, Map<String, Set<String>>>();
				
		for(Term term: schemaTerms.keySet()) {
			Set<Triple> triples = schemaTerms.get(term);
			Triple schemaTriple = triples.iterator().next();
			
			Map<String, String> where = GenericRule.getRule(schemaTriple).where(schemaTriple);
			
			Integer size = where.size();
			
			Map<String, Set<String>> conditions = parts.get(size);
			if(conditions == null) {
				conditions = new HashMap<String, Set<String>>();
				parts.put(size, conditions);
			}
			
			for(String triplePart: where.keySet()) {
				Set<String> conditionParts = conditions.get(triplePart);
				if(conditionParts == null) {
					conditionParts = new HashSet<String>();
					conditions.put(triplePart, conditionParts);
				}
				conditionParts.add(where.get(triplePart));
			}
		}
		
		StringBuilder query = new StringBuilder(SELECT_FROM).append(decoratedTable).append(" where ");
		Set<String> conditions = new HashSet<String>();
		
		Joiner joiner;
		
		for(Integer key: parts.keySet()) {
			
			Map<String, Set<String>> values =  parts.get(key);
			
			Set<String> conditionPart = new HashSet<String>();
			StringBuilder condition = new StringBuilder();
			
			for(String triplePart: values.keySet()) {
				Set<String> terms = values.get(triplePart);
				StringBuilder triplePartCondition = new StringBuilder(triplePart);
				if(terms.size() == 1) {
					triplePartCondition.append('=').append(terms.iterator().next());
				} else {
					joiner = Joiner.on(',');
					triplePartCondition.append(" IN (").append(joiner.join(terms)).append(')');
				}
				
				conditionPart.add(triplePartCondition.toString());
			}
			
			joiner = Joiner.on(" and ");
			condition.append('(').append(joiner.join(conditionPart)).append(')');
			conditions.add(condition.toString());
		}
		
		joiner = Joiner.on(" OR ");
		query.append(joiner.join(conditions)).append(';');
		
		log.debug("Query size: " + Utils.getStringSize(query.toString()));
		queries.add(query.toString());
		// TODO take care of query size
		
		return queries;
	}

	/**
	 * @param decoratedTable the decoratedTable to set
	 */
	public void setDecoratedTable(String decoratedTable) {
		this.decoratedTable = decoratedTable;
	}
	

}
