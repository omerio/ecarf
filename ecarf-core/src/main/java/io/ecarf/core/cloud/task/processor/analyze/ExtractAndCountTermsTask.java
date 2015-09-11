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


package io.ecarf.core.cloud.task.processor.analyze;

import io.ecarf.core.cloud.task.processor.ProcessLoadTask;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ExtractAndCountTermsTask extends ProcessLoadTask {


    private final static Log log = LogFactory.getLog(ExtractAndCountTermsTask.class);

    private Set<String> allTerms = new HashSet<String>();
    
    private String sourceBucket;


    /* (non-Javadoc)
     * @see io.cloudex.framework.Executable#run()
     */
    @Override
    public void run() throws IOException {
        log.info("Extracting all terms and counting schema terms");
        super.run();

        // upload all the extracted terms as well
        log.info("Saving all terms to file. Number of terms: " + allTerms.size());
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        String allTermsFile = Utils.TEMP_FOLDER + Constants.NODE_TERMS + 
                cloudService.getInstanceId() + Constants.DOT_SER + Constants.GZIP_EXT;
        
        Utils.objectToFile(allTermsFile, allTerms, true);
        
        //String compressedFile = Utils.compressFile(allTermsFile);
        
        cloudService.uploadFileToCloudStorage(allTermsFile, this.getBucket());

        log.info("TIMER# All files are processed and uploaded successfully " + stopwatch);
    }

    /*
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.processor.ProcessLoadTask1#getSubTasks(java.util.Set)
     */
    @Override
    public List<Callable<TermCounter>> getSubTasks(Set<String> files) {
        List<Callable<TermCounter>> tasks = new ArrayList<>();

        if(StringUtils.isBlank(sourceBucket)) {
            log.warn("sourceBucket is empty, using bucket: " + this.getBucket());
            this.sourceBucket = this.getBucket();
        }
        
        Set<String> schemaTerms = this.getSchemaTerms();

        for(final String file: files) {

            TermCounter counter = null;

            if(schemaTerms != null) {
                counter = new TermCounter();
                counter.setTermsToCount(schemaTerms);
            }

            ExtractAndCountTermsSubTask task = 
                    new ExtractAndCountTermsSubTask(file, this.sourceBucket, counter, this.getCloudService());

            tasks.add(task);

        }

        return tasks;
    }
    
    /*
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.processor.ProcessLoadTask1#processSingleOutput(io.ecarf.core.term.TermCounter)
     */
    @Override
    public void processSingleOutput(TermCounter counter) {

        super.processSingleOutput(counter);

        // get all terms as well
        if(counter != null) {
            this.allTerms.addAll(counter.getAllTerms());
        }

    }

    /*
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.processor.ProcessLoadTask1#processMultiOutput(java.util.List)
     */
    @Override
    public void processMultiOutput(List<TermCounter> counters) {

        super.processMultiOutput(counters);

        // get all terms as well
        for(TermCounter counter: counters) {
            if(counter != null) {
                this.allTerms.addAll(counter.getAllTerms());
            }
        }


    }

    /**
     * @return the allTerms
     */
    public Set<String> getAllTerms() {
        return allTerms;
    }

    /**
     * @return the sourceBucket
     */
    public String getSourceBucket() {
        return sourceBucket;
    }

    /**
     * @param sourceBucket the sourceBucket to set
     */
    public void setSourceBucket(String sourceBucket) {
        this.sourceBucket = sourceBucket;
    }

}
