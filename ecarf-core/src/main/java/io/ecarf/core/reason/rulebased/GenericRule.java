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

import io.ecarf.core.reason.rulebased.owl2rl.rdfs.CaxScoRule;
import io.ecarf.core.reason.rulebased.owl2rl.rdfs.PrpDomRule;
import io.ecarf.core.reason.rulebased.owl2rl.rdfs.PrpRngRule;
import io.ecarf.core.reason.rulebased.owl2rl.rdfs.PrpSpo1Rule;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.triple.TermType;
import io.ecarf.core.triple.Triple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public abstract class GenericRule implements Rule {
	
	public static final Map<String, Rule> RULES = new HashMap<>();
	
	static {	
		RULES.put(SchemaURIType.RDFS_DOMAIN.getUri(), new PrpDomRule());
		RULES.put(SchemaURIType.RDFS_RANGE.getUri(), new PrpRngRule());
		RULES.put(SchemaURIType.RDFS_SUBPROPERTY.getUri(), new PrpSpo1Rule());
		RULES.put(SchemaURIType.RDFS_SUBCLASS.getUri(), new CaxScoRule());
	}
	
	
	/**
	 * Return a SQL like query that can be used to retrieve all the instance
	 * triples that forms the second part of the body of this rule
	 * @param schemaTriple
	 * @param instanceTriple
	 * @return
	 */
	@Override
	public String query(Triple schemaTriple, String table) {
		return query(schemaTriple, table, null);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.reason.rulebased.Rule#query(io.ecarf.core.triple.Triple, 
	 * io.ecarf.core.triple.Triple, java.lang.String, java.util.Set)
	 */
	@Override
	public String query(Triple schemaTriple, String table, List<String> select) {
		/*
		 * select subject from swetodblp.swetodblp_triple where 
				 object = "<http://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings>"; */
		
		StringBuilder query = new StringBuilder("select ");
		
		select = (select == null) ? this.select() : select;
		
		Joiner joiner = Joiner.on(',');
		query.append(joiner.join(select)).append(" from ").append(table).append(" where ");
		
		Map<String, String> where = this.where(schemaTriple);
		
		joiner = Joiner.on(" and ");
		query.append(joiner.join(where.entrySet())).append(';');
		
		return query.toString();
	}
	
	/**
	 * Get a reasoning rule for the provided schema triple
	 * @param schemaTriple
	 * @return
	 */
	public static Rule getRule(Triple schemaTriple) {
		return RULES.get(schemaTriple.getPredicate());
	}
	
	/**
	 * Get the select columns for the provided triples which are grouped
	 * together by either property or class
	 * @param triples
	 * @return
	 */
	public static List<String> getSelect(Set<Triple> triples) {
		// make sure we eliminate duplicates, use a set
		Set<String> select = new HashSet<>();
		for(Triple triple: triples) {
			select.addAll(GenericRule.getRule(triple).select());
		}
		
		// make sure we add the terms in the order subject predicate object
		List<String> selects = new ArrayList<>();
		if(select.contains(TermType.subject)) {
			selects.add(TermType.subject);
		}
		if(select.contains(TermType.predicate)) {
			selects.add(TermType.predicate);
		}
		if(select.contains(TermType.object)) {
			selects.add(TermType.object);
		}
		return selects;
	}
	
	/**
	 * Get the query that can be used to retrieve instance triples for the provided
	 * triple which are grouped together by either property or class
	 * @param triples
	 * @return
	 */
	public static String getQuery(Set<Triple> triples, String table) {
		List<String> select = getSelect(triples);
		Triple schemaTriple = triples.iterator().next();
		
		Rule rule = getRule(schemaTriple);
		return rule.query(schemaTriple, table, select);
	}
	
	

}
