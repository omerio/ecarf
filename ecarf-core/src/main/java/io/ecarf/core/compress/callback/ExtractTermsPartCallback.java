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

package io.ecarf.core.compress.callback;

import io.ecarf.core.compress.NTripleGzipCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.term.TermRoot;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ExtractTermsPartCallback implements NTripleGzipCallback {

    private Set<String> resources = new HashSet<>();

    private Set<String> blankNodes = new HashSet<>();

    private TermCounter counter;
    
    private int literalCount;
    

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#process(org.semanticweb.yars.nx.Node[])
     */
    @Override
    public String process(Node[] nodes) throws IOException {
        
        String term;

        for (int i = 0; i < nodes.length; i++)  {

            // we are not going to unscape literals, these can contain new line and 
            // unscaping those will slow down the bigquery load, unless offcourse we use JSON
            // instead of CSV https://cloud.google.com/bigquery/preparing-data-for-bigquery
            if((i == 2) && (nodes[i] instanceof Literal)) {

                literalCount++;

            } else {
                
                //TODO if we are creating a dictionary why unscape this term anyway?
                //term = NxUtil.unescape(nodes[i].toN3());
                term = nodes[i].toN3();

                if(nodes[i] instanceof BNode) {
                    blankNodes.add(term);

                } else {
                    
                    if(!SchemaURIType.RDF_OWL_TERMS.contains(term)) {
                        
                        String url = term.substring(1, term.length() - 1);
                        String path = StringUtils.removeStart(url, TermRoot.HTTP);
                        
                        if(path.length() == url.length()) {
                            path = StringUtils.removeStart(path, TermRoot.HTTPS);
                        }
                        
                        //String [] parts = StringUtils.split(path, URI_SEP);
                        // this is alot faster than String.split or StringUtils.split
                        List<String> parts = Utils.split(path, TermRoot.URI_SEP);
                        
                        // invalid URIs, e.g. <http:///www.taotraveller.com> is parsed by NxParser as http:///
                        if(!parts.isEmpty()) {
                            resources.addAll(parts);
                        }
                    }
                   
                }
                
                //if(counter != null) {
                counter.count(term);
                //}
            }
        }

        return null;
    }

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#setCounter(io.ecarf.core.term.TermCounter)
     */
    @Override
    public void setCounter(TermCounter counter) {       
        this.counter = counter;
    }

    /**
     * @return the blankNodes
     */
    public Set<String> getBlankNodes() {
        return blankNodes;
    }

    /**
     * @return the literalCount
     */
    public int getLiteralCount() {
        return literalCount;
    }

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#setOutput(java.lang.Appendable)
     */
    @Override
    public void setOutput(Appendable out) throws IOException {
    }

    /**
     * @return the resources
     */
    public Set<String> getResources() {
        return resources;
    }

}
