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
	SUBJECT, OBJECT, PREDICATE;
	
	/**
	 * Get the term as lower case
	 * @return
	 */
	public String term() {
		return this.toString().toLowerCase();
	}
}
