/**
 * 
 */
package io.ecarf.core.triple;

/**
 * An enum that represents the terms of a triple 
 * 
 * @author omerio
 *
 */
public enum TermType {
	
	// don't change this order it's used for Big Data database
	SUBJECT, PREDICATE, OBJECT ;
	
	public static final String subject = "subject";
	public static final String predicate = "predicate";
	public static final String object = "object";
	/**
	 * Get the term as lower case
	 * @return
	 */
	public String term() {
		return this.toString().toLowerCase();
	}
}
