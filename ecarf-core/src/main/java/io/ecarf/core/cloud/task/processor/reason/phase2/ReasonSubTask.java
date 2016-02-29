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


package io.ecarf.core.cloud.task.processor.reason.phase2;

import io.ecarf.core.triple.Triple;
import io.ecarf.core.utils.Constants;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Carryout the reasoning for each file in a separate thread
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ReasonSubTask implements Callable<ReasonResult> {
    
    private boolean compressed;
    
    private String inFile;
    
    private Map<Long, Set<Triple>> schemaTerms;

    /**
     * @param compressed
     * @param inFile
     * @param schemaTerms
     */
    public ReasonSubTask(boolean compressed, String inFile,
            Map<Long, Set<Triple>> schemaTerms) {
        super();
        this.compressed = compressed;
        this.inFile = inFile;
        this.schemaTerms = schemaTerms;
    }

    /* (non-Javadoc)
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public ReasonResult call() throws Exception {
        
        String outFile =  inFile + Constants.DOT_INF;
        
        Set<Long> productiveTerms = new HashSet<>();

        int inferred = ReasonUtils.reason(inFile, outFile, compressed,  schemaTerms, productiveTerms);
        
        return new ReasonResult(outFile, productiveTerms, inferred);
    }

}
