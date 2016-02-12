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

import io.ecarf.core.compress.NxGzipCallback;
import io.ecarf.core.term.TermCounter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ExtractTermsCallback implements NxGzipCallback {

    private Set<String> resources = new HashSet<>();

    private Set<String> blankNodes = new HashSet<>();

    private TermCounter counter;

    private int literalCount;

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#setOutput(java.lang.Appendable)
     */
    @Override
    public void setOutput(Appendable out) throws IOException {

    }

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#process(org.semanticweb.yars.nx.Node[])
     */
    @Override
    public String processNTriple(Node[] nodes) throws IOException {
        String term;

        for (int i = 0; i < nodes.length; i++)  {

            // we are not going to unscape literals, these can contain new line and 
            // unscaping those will slow down the bigquery load, unless offcourse we use JSON
            // instead of CSV https://cloud.google.com/bigquery/preparing-data-for-bigquery
            if((nodes[i] instanceof Literal)) {

                literalCount++;

            } else {
                
                //TODO if we are creating a dictionary why unscape this term anyway?
                //term = NxUtil.unescape(nodes[i].toN3());
                term = nodes[i].toN3();

                if(nodes[i] instanceof BNode) {
                    blankNodes.add(term);

                } else {
                    resources.add(term);
                }
                
                if(counter != null) {
                    counter.count(term);
                }
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
     * @return the resources
     */
    public Set<String> getResources() {
        return resources;
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

    @Override
    public String processNQuad(Node[] nodes) throws IOException {
        return null;
    }

}
