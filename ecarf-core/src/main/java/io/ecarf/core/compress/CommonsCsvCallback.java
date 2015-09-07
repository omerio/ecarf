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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * 
 * An implementation of {@link NTripleGzipCallback} that is based on Apache Commons CSV
 * {@link CSVPrinter}
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class CommonsCsvCallback implements NTripleGzipCallback {
    
    private TermCounter counter;
    
    private CSVPrinter printer;

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#setOutput(java.lang.Appendable)
     */
    @Override
    public void setOutput(Appendable out) throws IOException {
        this.printer = new CSVPrinter(out, CSVFormat.DEFAULT);
    }

    /* (non-Javadoc)
     * @see io.ecarf.core.compress.NTripleGzipCallback#process(java.lang.String[])
     */
    @Override
    public String process(String[] terms) throws IOException {
        if(counter != null) {
            counter.count(terms);
        }
 
        this.printer.printRecord(terms);
        
        return null;
    }

    @Override
    public void setCounter(TermCounter counter) {
        this.counter = counter;
    }

}
