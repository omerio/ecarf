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


package io.ecarf.core.cloud.task.processor.reason.phase3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.cloudex.framework.cloud.entities.QueryStats;
import io.ecarf.core.cloud.task.processor.reason.phase2.DoReasonTask9;
import io.ecarf.core.cloud.task.processor.reason.phase2.ReasonResult;
import io.ecarf.core.cloud.task.processor.reason.phase2.ReasonUtils;
import io.ecarf.core.utils.Constants;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTask10 extends DoReasonTask9 {
    
    private final static Log log = LogFactory.getLog(DoReasonTask10.class);
    
    private DuplicatesBuster duplicatesBuster = new DuplicatesBuster();

    /* (non-Javadoc)
     * @see io.ecarf.core.cloud.task.processor.reason.phase2.DoReasonTask9#inferAndSaveTriplesToFile(io.cloudex.framework.cloud.entities.QueryStats, java.util.Set, int, java.util.Set)
     */
    @Override
    protected int inferAndSaveTriplesToFile(QueryStats stats, Set<Long> productiveTerms, int processors, Set<String> inferredTriplesFiles) throws IOException {

        log.info("********************** Starting Inference Round **********************");

        int inferredTriples = 0;

        boolean compressed = stats.getTotalRows().intValue() > this.ddLimit;

        List<String> files = stats.getOutputFiles();

        if(!files.isEmpty()) {

            String outFile;

            if((processors == 1) || (files.size() == 1)) {
                // if one file or one processor then reason serially 

                for(String file: files) {

                    outFile =  file + Constants.DOT_INF;

                    int inferred = ReasonUtils.reason(file, outFile, compressed,  schemaTerms, productiveTerms, duplicatesBuster);

                    if(inferred > 0) {
                        inferredTriplesFiles.add(outFile);
                    }

                    inferredTriples += inferred;
                }

            } else {

                // multiple cores
                List<ReasonSubTask> tasks = new ArrayList<>();

                for(String file: files) {
                    tasks.add(new ReasonSubTask(compressed, file, schemaTerms, duplicatesBuster));
                }

                try {
                    List<Future<ReasonResult>> results = executor.invokeAll(tasks);

                    for (Future<ReasonResult> result : results) {
                        ReasonResult reasonResult = result.get();

                        outFile =  reasonResult.getOutFile();

                        int inferred = reasonResult.getInferred();

                        productiveTerms.addAll(reasonResult.getProductiveTerms());

                        if(inferred > 0) {
                            inferredTriplesFiles.add(outFile);
                        }

                        inferredTriples += inferred;
                    }

                } catch(Exception e) {
                    log.error("Failed to run reasoning job in parallel", e);
                    executor.shutdown();
                    throw new IOException(e);
                }

            }
        }

        log.info("Total Rows: " + stats.getTotalRows() + 
                ", Total Processed Bytes: " + stats.getTotalProcessedGBytes() + " GB" + 
                ", Inferred: " + inferredTriples + ", compressed = " + compressed + 
                ", out files: " + inferredTriplesFiles.size());

        log.info("********************** Completed Inference Round **********************");

        return inferredTriples;
    }

}
