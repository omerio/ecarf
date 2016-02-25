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

import io.ecarf.core.reason.rulebased.GenericRule;
import io.ecarf.core.reason.rulebased.Rule;
import io.ecarf.core.triple.TermType;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
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
public class QueryGenerator<T> {
	
	private final static Log log = LogFactory.getLog(QueryGenerator.class);
	
	//private static final String SELECT_FROM = "select subject, predicate, object from ";
	
	private static final String SELECT = "select ";
	
	private static final String FROM = " from ";
	
	private String decoratedTable;
	private Map<T, Set<Triple>> schemaTerms; 

	/**
	 * 
	 */
	public QueryGenerator(Map<T, Set<Triple>> schemaTerms, String decoratedTable) {
		this.decoratedTable = decoratedTable;
		this.schemaTerms = schemaTerms;
	}
	
	public List<String> getQueries() {
		List<String> queries = new ArrayList<String>();
		
		Map<Integer, Map<String, Set<String>>> parts = new HashMap<Integer, Map<String, Set<String>>>();
		Set<String> selects = new HashSet<>();
		boolean encoded = false;
				
		for(T term: schemaTerms.keySet()) {
			Set<Triple> triples = schemaTerms.get(term);
			
			Triple schemaTriple = null;
			Rule rule = null;
			
			for(Triple sTriple: triples) {
			    rule = GenericRule.getRule(sTriple);
			    selects.addAll(rule.select());
			    if(schemaTriple == null) {
			        schemaTriple = sTriple;
			        encoded = schemaTriple.isEncoded();
			    }
			}
			
			Map<String, String> where = rule.where(schemaTriple);
			
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
		
		selects.add(TermType.predicate);
		
		// only add the object_literal if the triple is encoded
		if(!encoded) {
		    selects.remove(TermType.object_literal);
		}
		
		String columns = StringUtils.join(GenericRule.getOrderedSelect(selects), ", ");
		
		StringBuilder query = new StringBuilder(SELECT).append(columns)
		    .append(FROM).append(decoratedTable).append(" where ");
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
