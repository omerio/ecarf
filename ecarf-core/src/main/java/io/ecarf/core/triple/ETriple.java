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
 * Encoded Triple
 * @author omerio
 *
 */
public class ETriple implements Triple {
	
	private static final Map<String, SchemaURIType> MAPPINGS = SchemaURIType.mappings();
	
	private Long subject;
	
	private Long predicate;
	
	private Long object;
	
	private String objectLiteral;
	
	private boolean inferred;
	
	
	/**
	 * 
	 */
	public ETriple() {
		super();
	}



	/**
	 * 
	 * @param subject
	 * @param predicate
	 * @param object
	 */
	public ETriple(Long subject, Long predicate, Long object, String objectLiteral) {
		super();
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.objectLiteral = objectLiteral;
	}
	
	

	/**
	 * @param subject
	 * @param predicate
	 * @param object
	 * @param inferred
	 */
	public ETriple(Long subject, Long predicate, Long object, String objectLiteral,
			boolean inferred) {
		super();
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.objectLiteral = objectLiteral;
		this.inferred = inferred;
	}
	
	 @Override
	    public Triple create(Object subject, Object predicate, Object object) {
	        return new ETriple((Long) subject, (Long) predicate, (Long) object, null);
	    }
	
	/**
	 * Set a field with the provided value
	 * @param field
	 * @param value
	 */
	public void set(String field, Object value) {
		switch(field) {
		case TermType.subject:
			this.setSubject((Long) value);
			break;
		case TermType.predicate:
			this.setPredicate((Long) value);
			break;
		case TermType.object:
			this.setObject((Long) value);
			break;
			
		}
	}
	
	/**
	 * Populate from a list of values
	 * @param fields
	 * @param values
	 */
	public void set(List<String> fields, Long... values) {
		Preconditions.checkArgument(fields.size() == values.length, "Select terms must be equal to values selected");
		for(int i = 0; i < values.length; i++) {
			this.set(fields.get(i), values[i]);
		}
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
	 * Create a new triple from a CSV line
	 * @param terms
	 * @return
	 */
	public static ETriple fromCSV(String[] terms) {
	    ETriple triple = new ETriple();
	    
	    
	    if(terms[0] != null) {
	        triple.setSubject(Long.parseLong(terms[0]));
	    }
	    
	    if(terms[1] != null) {
	        triple.setPredicate(Long.parseLong(terms[1]));
	    }
	    
	    if(terms[2] != null) {
	        triple.setObject(Long.parseLong(terms[2]));
	    }
	    
	    if(terms.length > 3) {
	        // we have object literal
	        triple.setObjectLiteral(terms[3]);
	    }
	    //new ETriple(terms[0], terms[1], terms[2]);
		return triple;
	}
	
	/**
	 * Return matching triples for a criteria
	 * @param term
	 * @param type
	 * @param uri
	 * @param triples
	 * @return
	 */
	public static Set<ETriple> getTriplesForCriteria(String term, TermType type, String uri, Set<ETriple> triples) {
		Set<ETriple> matched = new HashSet<>();
		for(ETriple triple: triples) {
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
	public static Set<ETriple> getTriplesForCriteria(String term, TermType type, String uri, 
			boolean inferred, Set<ETriple> triples) {
		
		Set<ETriple> matched = new HashSet<>();
		for(ETriple triple: triples) {
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
		ETriple rhs = (ETriple) obj;
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
	public ETriple clone() {
		ETriple triple = new ETriple(this.subject, this.predicate, this.object, this.objectLiteral);
		triple.setInferred(this.inferred);
		
		return triple;
	}
	
	/**
	 * Convert to NTriple format
	 * @return
	 */
	public String toNTriple() {
		return null;
		
		/*new StringBuilder(this.subject).append(' ')
				.append(this.predicate).append(' ')
				.append(this.object).append(" .").toString();*/
	}
	
	/**
	 * Convert to CSV
	 * TODO add inferred as well
	 * @return
	 */
	public String toCsv() {
		 return new StringBuilder()
		            .append(this.subject).append(',')
					.append(this.predicate).append(',')
					.append(this.object).append(',')
					.append(StringEscapeUtils.escapeCsv(this.objectLiteral))
					.toString();
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



    @Override
    public boolean isEncoded() {
        return true;
    }



    /**
     * @return the subject
     */
    public Long getSubject() {
        return subject;
    }



    /**
     * @param subject the subject to set
     */
    public void setSubject(Long subject) {
        this.subject = subject;
    }



    /**
     * @return the predicate
     */
    public Long getPredicate() {
        return predicate;
    }



    /**
     * @param predicate the predicate to set
     */
    public void setPredicate(Long predicate) {
        this.predicate = predicate;
    }



    /**
     * @return the object
     */
    public Long getObject() {
        return object;
    }



    /**
     * @param object the object to set
     */
    public void setObject(Long object) {
        this.object = object;
    }



    /**
     * @return the objectLiteral
     */
    public String getObjectLiteral() {
        return objectLiteral;
    }



    /**
     * @param objectLiteral the objectLiteral to set
     */
    public void setObjectLiteral(String objectLiteral) {
        this.objectLiteral = objectLiteral;
    }
    
    public boolean isInferred() {
        return inferred;
    }

    public void setInferred(boolean inferred) {
        this.inferred = inferred;
    }

}
