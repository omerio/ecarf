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
package io.ecarf.core.cloud.task.processor;

import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.cloud.task.processor.files.ProcessFilesTask;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.term.dictionary.TermDictionary;
import io.ecarf.core.term.dictionary.TermDictionaryCore;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.FilenameUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * EVM Task to load the provided cloud files into the big data cloud storage
 * Does also analyse the terms as they are being processed
 * read the files from http:// or from gs://
 * download files locally (gziped)
 * read through the files counting the relevant terms and rewriting 
 * into bigquery format (comma separated)
 * Makes use of CPU multi-cores for parallel processing
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ProcessLoadTask extends ProcessFilesTask<TermCounter> {

    private final static Log log = LogFactory.getLog(ProcessLoadTask.class);

    private String bucket;
    
    private String sourceBucket;

    private String schemaTermsFile;
    
    private String countOnly;
    
    private String encode;

    private Set<String> schemaTerms;
    
    private Map<String, Integer> count = new HashMap<>();
    
    private TermDictionary dictionary;
    
    private String dictionaryFile;

    /* 
     * // TODO distinguish between files in cloud storage vs files downloaded from http or https url
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.Task#run()
     */
    @Override
    public void run() throws IOException {

        log.info("START: processing files, memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: 0s");
        Stopwatch stopwatch = Stopwatch.createStarted();

        EcarfGoogleCloudService cloudService = (EcarfGoogleCloudService) this.getCloudService();
        
        log.info("Downloading schema terms file: " + schemaTermsFile);
        
        this.schemaTerms = cloudService.getSetFromCloudStorageFile(schemaTermsFile, bucket);
        
        if(Boolean.valueOf(encode) && StringUtils.isNotBlank(this.dictionaryFile)) {
            
            log.info("Downloading and loading dictionary into memory from file: " + this.dictionaryFile);
            
            // 1- Download the dictionary
            String localFile = Utils.TEMP_FOLDER + dictionaryFile;  
            
            if(FilenameUtils.fileExists(localFile)) {
                log.info("Re-using local file: " + localFile);
                
            } else {
                this.cloudService.downloadObjectFromCloudStorage(dictionaryFile, localFile, this.bucket);
            }

            log.info("Loading the dictionary from file: " + localFile + ", memory usage: " + 
                    Utils.getMemoryUsageInGB() + "GB, timer: " + stopwatch);

            // 2- Load the dictionary
            try {
                
                dictionary = Utils.objectFromFile(localFile, TermDictionaryCore.class, true, false);
                
            } catch (ClassNotFoundException e) {
                
                log.error("Failed to load the dictionary", e);
                throw new IOException(e);
            }
            
            log.info("Dictionary loaded successfully, memory usage: " + 
                    Utils.getMemoryUsageInGB() + "GB, timer: " + stopwatch);
        }

        super.run();

        // write term stats to file and upload
        if(!count.isEmpty()) {
            
            log.info("Saving terms stats");
            String countStatsFile = Utils.TEMP_FOLDER + cloudService.getInstanceId() + Constants.DOT_JSON;
            FileUtils.objectToJsonFile(countStatsFile, count);

            cloudService.uploadFileToCloudStorage(countStatsFile, bucket);
        }

        log.info("FINISH: All files are processed and uploaded successfully,, memory usage: " + 
                Utils.getMemoryUsageInGB() + "GB, timer: " + stopwatch);
    }
    

    /**
     * Return all the process tasks
     */
    @Override
    public List<Callable<TermCounter>> getSubTasks(Set<String> files) {
        List<Callable<TermCounter>> tasks = new ArrayList<>();
        
        if(StringUtils.isBlank(sourceBucket)) {
            log.warn("sourceBucket is empty, using bucket: " + this.bucket);
            this.sourceBucket = this.bucket;
        }
        
        for(final String file: files) {

            TermCounter counter = null;

            if(schemaTerms != null) {
                counter = new TermCounter();
                counter.setTermsToCount(schemaTerms);
            }

            ProcessFilesForBigQuerySubTask task = 
                    new ProcessFilesForBigQuerySubTask(file, bucket, sourceBucket, 
                            counter, this.dictionary, Boolean.valueOf(countOnly), Boolean.valueOf(encode), this.getCloudService());
            tasks.add(task);

        }

        return tasks;
    }

    /*
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.processor.files.ProcessFilesTask#processSingleOutput(java.lang.Object)
     */
    @Override
    public void processSingleOutput(TermCounter counter) {
        if(counter != null) {
            count = counter.getCount();
        }

    }

    /*
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.processor.files.ProcessFilesTask#processMultiOutput(java.util.List)
     */
    @Override
    public void processMultiOutput(List<TermCounter> counters) {
        for(TermCounter counter: counters) {
            if(counter != null) {
                Utils.mergeCountMaps(count, counter.getCount());
            }
        }
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
     * @return the schemaTermsFile
     */
    public String getSchemaTermsFile() {
        return schemaTermsFile;
    }

    /**
     * @param schemaTermsFile the schemaTermsFile to set
     */
    public void setSchemaTermsFile(String schemaTermsFile) {
        this.schemaTermsFile = schemaTermsFile;
    }

    /**
     * @return the schemaTerms
     */
    public Set<String> getSchemaTerms() {
        return schemaTerms;
    }

    /**
     * @return the count
     */
    public Map<String, Integer> getCount() {
        return count;
    }

    /**
     * @return the countOnly
     */
    public String getCountOnly() {
        return countOnly;
    }

    /**
     * @param countOnly the countOnly to set
     */
    public void setCountOnly(String countOnly) {
        this.countOnly = countOnly;
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

    /**
     * @return the encode
     */
    public String getEncode() {
        return encode;
    }

    /**
     * @param encode the encode to set
     */
    public void setEncode(String encode) {
        this.encode = encode;
    }

    /**
     * @return the dictionaryFile
     */
    public String getDictionaryFile() {
        return dictionaryFile;
    }

    /**
     * @param dictionaryFile the dictionaryFile to set
     */
    public void setDictionaryFile(String dictionaryFile) {
        this.dictionaryFile = dictionaryFile;
    }

}
