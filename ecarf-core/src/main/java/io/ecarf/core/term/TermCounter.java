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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TermCounter {
	
	// count only these terms
	private Set<String> termsToCount;
	
	// total count
	private Map<String, Integer> count = new HashMap<>();
	
	private Set<String> allTerms = new HashSet<>();
	
	/**
	 * Count the terms for the provided triple terms
	 * @param terms
	 */
	public void count(String [] terms) {
		if(terms != null) {

			for(String term: terms) {
				
				// either selective count or count everything
				if((this.termsToCount == null) || 
						((termsToCount != null) && this.termsToCount.contains(term))) {
					this.countTerm(term);					
				} 
			}
			
		}
	}
	
	/**
	 * Count a single term
	 * @param term
	 */
	public void count(String term) {
	    // either selective count or count everything
	    if((this.termsToCount == null) || 
	            ((termsToCount != null) && this.termsToCount.contains(term))) {
	        this.countTerm(term);                   
	    } 
	}
	
	/**
	 * count the provided term 
	 * @param term
	 */
	private void countTerm(String term) {

		if(!count.containsKey(term)) {
			count.put(term, 0);
		}

		count.put(term, count.get(term) + 1);
	}

	/**
	 * Add a term to this counter's allTerms set
	 * @param term
	 */
	public void addTerm(String term) {
	    this.allTerms.add(term);
	}
	
	/**
	 * @return the termsToCount
	 */
	public Set<String> getTermsToCount() {
		return termsToCount;
	}

	/**
	 * @param termsToCount the termsToCount to set
	 */
	public void setTermsToCount(Set<String> termsToCount) {
		this.termsToCount = termsToCount;
	}

	/**
	 * @return the count
	 */
	public Map<String, Integer> getCount() {
		return count;
	}

	/**
	 * @param count the count to set
	 */
	public void setCount(Map<String, Integer> count) {
		this.count = count;
	}

    /**
     * @return the allTerms
     */
    public Set<String> getAllTerms() {
        return allTerms;
    }

    /**
     * @param allTerms the allTerms to set
     */
    public void setAllTerms(Set<String> allTerms) {
        this.allTerms = allTerms;
    }


}
