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

import java.util.Set;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ReasonResult {
    
    private String outFile;
    
    private Set<Long> productiveTerms;

    private int inferred;
    
    

    /**
     * @param outFile
     * @param productiveTerms
     * @param inferred
     */
    public ReasonResult(String outFile, Set<Long> productiveTerms, int inferred) {
        super();
        this.outFile = outFile;
        this.productiveTerms = productiveTerms;
        this.inferred = inferred;
    }

    /**
     * @return the outFile
     */
    public String getOutFile() {
        return outFile;
    }

    /**
     * @param outFile the outFile to set
     */
    public void setOutFile(String outFile) {
        this.outFile = outFile;
    }

    /**
     * @return the productiveTerms
     */
    public Set<Long> getProductiveTerms() {
        return productiveTerms;
    }

    /**
     * @param productiveTerms the productiveTerms to set
     */
    public void setProductiveTerms(Set<Long> productiveTerms) {
        this.productiveTerms = productiveTerms;
    }

    /**
     * @return the inferred
     */
    public int getInferred() {
        return inferred;
    }

    /**
     * @param inferred the inferred to set
     */
    public void setInferred(int inferred) {
        this.inferred = inferred;
    } 

}
