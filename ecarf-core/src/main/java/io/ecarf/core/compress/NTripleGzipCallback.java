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
package io.ecarf.core.compress;

import io.ecarf.core.term.TermCounter;

import java.io.IOException;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface NTripleGzipCallback {
    
    /**
     * The output if we are directly appending to an output
     * @param out
     */
    public void setOutput(Appendable out) throws IOException;
	
    /**
     * Do actual processing for the provided terms array
     * @param line
     * @return
     * @throws IOException
     */
	public String process(String[] line) throws IOException;
	
	/**
	 * Set a term counter if we are counting the terms as well
	 * @param counter
	 */
	public void setCounter(TermCounter counter);

}
