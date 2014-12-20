/**
 * 
 */
package io.ecarf.core.triple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

import com.google.common.base.Preconditions;

/**
 * @author omerio
 *
 */
public class Triple {
	
	private static final Map<String, SchemaURIType> MAPPINGS = SchemaURIType.mappings();
	
	private String subject;
	
	private String predicate;
	
	private String object;
	
	private boolean inferred;
	
	
	/**
	 * 
	 */
	public Triple() {
		super();
	}



	/**
	 * 
	 * @param subject
	 * @param predicate
	 * @param object
	 */
	public Triple(String subject, String predicate, String object) {
		super();
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}
	
	

	/**
	 * @param subject
	 * @param predicate
	 * @param object
	 * @param inferred
	 */
	public Triple(String subject, String predicate, String object,
			boolean inferred) {
		super();
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.inferred = inferred;
	}
	
	/**
	 * Set a field with the provided value
	 * @param field
	 * @param value
	 */
	public void set(String field, String value) {
		switch(field) {
		case TermType.subject:
			this.setSubject(value);
			break;
		case TermType.predicate:
			this.setPredicate(value);
			break;
		case TermType.object:
			this.setObject(value);
			break;
			
		}
	}
	
	/**
	 * Populate from a list of values
	 * @param fields
	 * @param values
	 */
	public void set(List<String> fields, String... values) {
		Preconditions.checkArgument(fields.size() == values.length, "Select terms must be equal to values selected");
		for(int i = 0; i < values.length; i++) {
			this.set(fields.get(i), values[i]);
		}
	}



	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getPredicate() {
		return predicate;
	}

	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}

	public String getObject() {
		return object;
	}

	public void setObject(String object) {
		this.object = object;
	}

	public boolean isInferred() {
		return inferred;
	}

	public void setInferred(boolean inferred) {
		this.inferred = inferred;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public TermType getTermType(String key) {
		TermType type = null;
		
		if(this.subject.equals(key)) {
			type = TermType.SUBJECT;
		} else if(this.predicate.equals(key)) {
			type = TermType.PREDICATE;
		} else if(this.object.equals(key)) {
			type = TermType.OBJECT;
		}
		
		return type;
	}
	
	/**
	 * Return matching triples for a criteria
	 * @param term
	 * @param type
	 * @param uri
	 * @param triples
	 * @return
	 */
	public static Set<Triple> getTriplesForCriteria(String term, TermType type, String uri, Set<Triple> triples) {
		Set<Triple> matched = new HashSet<>();
		for(Triple triple: triples) {
			if(triple.isMatchPredicate(uri) && type.equals(triple.getTermType(term))) {
				matched.add(triple);
			}
		}
		return matched;
	}
	
	/**
	 * Return matching triples for a criteria
	 * @param term
	 * @param type
	 * @param uri
	 * @param triples
	 * @return
	 */
	public static Set<Triple> getTriplesForCriteria(String term, TermType type, String uri, 
			boolean inferred, Set<Triple> triples) {
		
		Set<Triple> matched = new HashSet<>();
		for(Triple triple: triples) {
			if(triple.isMatchPredicate(uri) && type.equals(triple.getTermType(term)) && (triple.isInferred() == inferred)) {
				matched.add(triple);
			}
		}
		return matched;
	}
	
	/**
	 * 
	 * @param uri
	 * @return
	 */
	public boolean isMatchPredicate(String uri) {
		return this.predicate.equals(uri);
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean isSchemaPredicate() {
		return MAPPINGS.get(this.predicate) != null;
	}
	
	/**
	 * 
	 * @return
	 */
	public SchemaURIType getSchemaURIType() {
		return MAPPINGS.get(this.predicate);
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean isRdfType() {
		return SchemaURIType.RDF_TYPE.getUri().equals(this.predicate);
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean isRdfsSubClassOf() {
		return SchemaURIType.RDFS_SUBCLASS.getUri().equals(this.predicate);
	}



	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((object == null) ? 0 : object.hashCode());
		result = prime * result
				+ ((predicate == null) ? 0 : predicate.hashCode());
		result = prime * result + ((subject == null) ? 0 : subject.hashCode());
		return result;
	}



	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == null) { return false; }
		if (obj == this) { return true; }
		if (obj.getClass() != getClass()) {
			return false;
		}
		Triple rhs = (Triple) obj;
		return new EqualsBuilder()
		.append(this.subject, rhs.subject)
		.append(this.predicate, rhs.predicate)
		.append(this.object, rhs.object)
		.isEquals();
	}



	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Triple [" + subject + " " + predicate + " " + object  + "]" + ", inferred=" + inferred;
	}
	
	/**
	 * Make a clone of this triple
	 */
	public Triple clone() {
		Triple triple = new Triple(this.subject, this.predicate, this.object);
		triple.setInferred(this.inferred);
		
		return triple;
	}
	
	/**
	 * Convert to NTriple format
	 * @return
	 */
	public String toNTriple() {
		return new StringBuilder(this.subject).append(' ')
				.append(this.predicate).append(' ')
				.append(this.object).append(" .").toString();
	}
	
	/**
	 * Convert to CSV
	 * TODO add inferred as well
	 * @return
	 */
	public String toCsv() {
		 return new StringBuilder(StringEscapeUtils.escapeCsv(this.subject)).append(',')
					.append(StringEscapeUtils.escapeCsv(this.predicate)).append(',')
					.append(StringEscapeUtils.escapeCsv(this.object)).toString();
	}
	
	/**
	 * Convert to a hashmap, easy to serialize as json
	 * @return
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>();
		map.put(TermType.subject, this.subject);
		map.put(TermType.predicate, this.predicate);
		map.put(TermType.object, this.object);
		return map;
	}

}
