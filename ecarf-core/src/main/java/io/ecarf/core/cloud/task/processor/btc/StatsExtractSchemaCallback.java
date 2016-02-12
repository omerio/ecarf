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

package io.ecarf.core.cloud.task.processor.btc;

import io.ecarf.core.compress.NxGzipCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.term.TermUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Triple;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class StatsExtractSchemaCallback implements NxGzipCallback {

    private Set<String> schemaTriples = new HashSet<>();
    
    private int literals;
    
    private int statements;
    
    private int blankNodes;
    

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#process(org.semanticweb.yars.nx.Node[])
     */
    @Override
    public String processNQuad(Node[] nodes) throws IOException {
        
        statements++;

        for (int i = 0; i < nodes.length; i++)  {

            if((nodes[2] instanceof Literal)) {
                literals++;
            } 

            if(nodes[0] instanceof BNode || nodes[2] instanceof BNode) {
                blankNodes++;
            }  

            String predicate = nodes[1].toN3();
            //NxUtil.unescape(ns[i].toN3());

            // do we have a schema term?
            if(TermUtils.RDFS_TBOX.contains(predicate)) {
                
                schemaTriples.add((new Triple(nodes[0], nodes[1], nodes[2])).toN3());
            }

 
        }

        return null;
    }

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#setCounter(io.ecarf.core.term.TermCounter)
     */
    @Override
    public void setCounter(TermCounter counter) {       
       
    }


    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#setOutput(java.lang.Appendable)
     */
    @Override
    public void setOutput(Appendable out) throws IOException {
    }

    @Override
    public String processNTriple(Node[] nodes) throws IOException {
        return null;
    }

    /**
     * @return the literals
     */
    public int getLiterals() {
        return literals;
    }

    /**
     * @return the statements
     */
    public int getStatements() {
        return statements;
    }

    /**
     * @return the blankNodes
     */
    public int getBlankNodes() {
        return blankNodes;
    }

    /**
     * @return the schemaTriples
     */
    public Set<String> getSchemaTriples() {
        return schemaTriples;
    }

}
