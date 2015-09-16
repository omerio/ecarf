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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A multi-threaded version that extract and counts all the terms using executor service
 * Terms are extracted into a TermRoot term tree which is then saved to a file. Blank nodes
 * are also saved separately for each processed file
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ExtractCountTermsPartTask extends ProcessLoadTask {


    private final static Log log = LogFactory.getLog(ExtractCountTermsPartTask.class);
    
    private String sourceBucket;


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

            ExtractCountTermsPartSubTask task = 
                    new ExtractCountTermsPartSubTask(file, this.getBucket(), this.sourceBucket, counter, this.getCloudService());

            tasks.add(task);

        }

        return tasks;
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
