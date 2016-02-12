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

import io.ecarf.core.cloud.task.processor.files.ProcessFilesTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Process a number of Nx files and extract the schema data
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ProcessNxFilesTask extends ProcessFilesTask<Void> {
    
    private final static Log log = LogFactory.getLog(ProcessNxFilesTask.class);
    
    private String sourceBucket;

    private String bucket;


    /* 
     * // TODO distinguish between files in cloud storage vs files downloaded from http or https url
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.Task#run()
     */
    /*@Override
    public void run() throws IOException {

        log.info("START: processing files, timer: 0s");
        Stopwatch stopwatch = Stopwatch.createStarted();

        EcarfGoogleCloudService cloudService = (EcarfGoogleCloudService) this.getCloudService();
        
        log.info("Downloading schema terms file: " + schemaTermsFile);
        
        this.schemaTerms = cloudService.getSetFromCloudStorageFile(schemaTermsFile, bucket);

        super.run();

        // write term stats to file and upload
        if(!count.isEmpty()) {
            
            log.info("Saving terms stats");
            String countStatsFile = Utils.TEMP_FOLDER + cloudService.getInstanceId() + Constants.DOT_JSON;
            FileUtils.objectToJsonFile(countStatsFile, count);

            cloudService.uploadFileToCloudStorage(countStatsFile, bucket);
        }

        log.info("FINISH: All files are processed and uploaded successfully, timer: " + stopwatch);
    }*/

    /**
     * Return all the process tasks
     */
    @Override
    public List<Callable<Void>> getSubTasks(Set<String> files) {
        List<Callable<Void>> tasks = new ArrayList<>();
        
        if(StringUtils.isBlank(sourceBucket)) {
            log.warn("sourceBucket is empty, using bucket: " + this.getBucket());
            this.sourceBucket = this.getBucket();
        }
        
        for(final String file: files) {

            ProcessNxFilesSubTask task = 
                    new ProcessNxFilesSubTask(file, this.getBucket(), this.sourceBucket, this.getCloudService());

            tasks.add(task);

        }

        return tasks;
    }

    /*
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.processor.files.ProcessFilesTask#processSingleOutput(java.lang.Object)
     */
    @Override
    public void processSingleOutput(Void nothing) {
       
    }

    /*
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.processor.files.ProcessFilesTask#processMultiOutput(java.util.List)
     */
    @Override
    public void processMultiOutput(List<Void> nothing) {
        
    }

    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * @param bucket the bucket to set
     */
    public void setBucket(String bucket) {
        this.bucket = bucket;
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
