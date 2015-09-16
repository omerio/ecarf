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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TermPart implements Serializable {
    
    private static final long serialVersionUID = -1364590982447745055L;

    private String term;
    
    private Map<String, TermPart> children;
    
    /**
     * 
     */
    public TermPart() {
        super();
    }
    
    /**
     * @param term
     */
    public TermPart(String term) {
        this();
        this.term = term;
    }
    
    /**
     * Add the elements of this array as children nested according to their order in the array
     * @param parts
     */
    public void addChildren(List<String> parts) {
        
        if(this.children == null) {
            this.children = new HashMap<>();
        }

        String part = parts.remove(0);
        TermPart termPart = this.children.get(part);

        if(termPart == null) {
            termPart = new TermPart(part);
            this.children.put(part, termPart);
        }
        
        // do we have children
        if(parts.size() > 1) {
            termPart.addChildren(parts);
        }
    }
    
    /**
     * Determine if this term part has childern, if not then it's at the moment 
     * the last part of the term
     * @return
     */
    public boolean hasChildren() {
        return (this.children != null) && !this.children.isEmpty();
    }
    
    /**
     * @return the term
     */
    public String getTerm() {
        return term;
    }

    /**
     * @param term the term to set
     */
    public void setTerm(String term) {
        this.term = term;
    }

    /**
     * @return the children
     */
    public Map<String, TermPart> getChildren() {
        return children;
    }

    /**
     * @param children the children to set
     */
    public void setChildren(Map<String, TermPart> children) {
        this.children = children;
    }
    
    /**
     * @return
     * @see java.util.Map#size()
     */
    public int size() {
        return children != null ? children.size() : 0;
    }

    /**
     * @return
     * @see java.util.Map#values()
     */
    public Collection<TermPart> values() {
        return children.values();
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
        .append("term", term)
        .append("children", children)
        .toString();
    }
        
}
