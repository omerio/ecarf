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

import java.io.IOException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.util.NxUtil;

/**
 * An implementation of {@link NTripleGzipCallback} that is based on Apache Commons
 * {@link StringEscapeUtils}
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class StringEscapeCallback implements NTripleGzipCallback {
    
    private TermCounter counter;

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
    public String process(Node[] nodes) throws IOException {
        
        String[] terms = new String [3];
        
        for (int i = 0; i < nodes.length; i++)  {
            
            // we are not going to unscape literals, these can contain new line and 
            // unscaping those will slow down the bigquery load, unless offcourse we use JSON
            // instead of CSV https://cloud.google.com/bigquery/preparing-data-for-bigquery
            if(nodes[i] instanceof Literal) {
                
                terms[i] = nodes[i].toN3();
                
            } else {
                terms[i] = NxUtil.unescape(nodes[i].toN3());
            }
        }
        
        if(counter != null) {
            counter.count(terms);
        }

        for(int i = 0; i < terms.length; i++) {
            // bigquery requires data to be properly escaped
            terms[i] = StringEscapeUtils.escapeCsv(terms[i]);
        }

        return StringUtils.join(terms, ',');
    }

    @Override
    public void setCounter(TermCounter counter) {
        this.counter = counter;
    }

}
