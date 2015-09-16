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

import io.ecarf.core.triple.SchemaURIType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TermRoot implements Serializable {

    private static final long serialVersionUID = 4875556040280460364L;
    
    private Map<String, TermPart> terms = new HashMap<>();
    
    private static final String HTTP = "http://";
    private static final String HTTPS = "https://";
    private static final char URI_SEP = '/';
    
    /**
     * Break down and add a term to this TermRoot
     * <http://dblp.uni-trier.de/rec/bibtex/books/mk/WidomC96>
     * @param term
     */
    public void addTerm(String term) {
        
        if(!SchemaURIType.RDF_OWL_TERMS.contains(term)) {
            
            String url = term.substring(1, term.length() - 1);
            String path = StringUtils.removeStart(url, HTTP);
            
            if(path.length() == url.length()) {
                path = StringUtils.removeStart(path, HTTPS);
            }
            
            //String [] parts = StringUtils.split(path, URI_SEP);
            // this is alot faster than String.split or StringUtils.split
            List<String> parts = new ArrayList<String>();
            int pos = 0, end;
            while ((end = path.indexOf(URI_SEP, pos)) >= 0) {
                parts.add(path.substring(pos, end));
                pos = end + 1;
            }
            
            String domain = parts.remove(0);

            if(!(TermUtils.equals(domain, SchemaURIType.W3_DOMAIN) && term.contains(SchemaURIType.LIST_EXPANSION_URI))) {
                TermPart termPart = this.terms.get(domain);

                if(termPart == null) {
                    termPart = new TermPart(domain);
                    this.terms.put(domain, termPart);
                }

                // do we have children
                if(parts.size() > 1) {
                    termPart.addChildren(parts);
                }
            } // else RDF list expansion e.g. <http://www.w3.org/1999/02/22-rdf-syntax-ns#_15>

        } // else a schema URI
    }

    /**
     * @return the terms
     */
    public Map<String, TermPart> getTerms() {
        return terms;
    }

    /**
     * @param terms the terms to set
     */
    public void setTerms(Map<String, TermPart> terms) {
        this.terms = terms;
    }
    
    /**
     * @return
     * @see java.util.Map#size()
     */
    public int size() {
        return terms.size();
    }

    /**
     * @return
     * @see java.util.Map#values()
     */
    public Collection<TermPart> values() {
        return terms.values();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
        .append("terms", terms)
        .toString();
    }

}
