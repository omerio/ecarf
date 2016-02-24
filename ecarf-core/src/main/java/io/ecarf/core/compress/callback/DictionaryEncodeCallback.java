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
import io.ecarf.core.term.dictionary.TermDictionary;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

/**
 * An implementation of {@link NxGzipCallback} that performs dictionary encoding, creates
 * a CSV line of 4 values: subject, predicate, object, object_literal
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DictionaryEncodeCallback implements NxGzipCallback {

    private TermDictionary dictionary;

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#setOutput(java.lang.Appendable)
     */
    @Override
    public void setOutput(Appendable out) {
    }

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#process(java.lang.String[])
     */
    @Override
    public String processNTriple(Node[] nodes) throws IOException {

        String[] terms = new String [4];

        String term;
        long enc = 0;

        for (int i = 0; i < nodes.length; i++)  {

            // we are not going to unscape literals, these can contain new line and 
            // unscaping those will slow down the bigquery load, unless offcourse we use JSON
            // instead of CSV https://cloud.google.com/bigquery/preparing-data-for-bigquery
            if((i == 2) && (nodes[i] instanceof Literal)) {

                terms[3] = nodes[i].toN3();

            } else {

                //TODO if we are creating a dictionary why unscape this term anyway?
                //term = NxUtil.unescape(nodes[i].toN3());
                term = nodes[i].toN3();

                if(nodes[i] instanceof BNode) {
                    
                    enc = dictionary.encodeBlankNode(term);
                    
                } else {

                    enc = dictionary.encode(term);
                }
                
                terms[i] = Long.toString(enc);


            }
        }

        return StringUtils.join(terms, ',');
    }

    @Override
    public void setCounter(TermCounter counter) {

    }

    @Override
    public String processNQuad(Node[] nodes) throws IOException {
        return null;
    }

    /**
     * @param dictionary the dictionary to set
     */
    public void setDictionary(TermDictionary dictionary) {
        this.dictionary = dictionary;
    }

}
