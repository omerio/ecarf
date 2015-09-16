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

import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.utils.FileUtils;
import io.cloudex.framework.utils.ObjectUtils;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.compress.NTripleGzipProcessor;
import io.ecarf.core.compress.callback.ExtractTermsCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * A single thread version that extract and count all the terms using one thread at a time
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ExtractAndCountTermsTask extends CommonTask {


    private final static Log log = LogFactory.getLog(ExtractAndCountTermsTask.class);

    private String files;

    private String sourceBucket;

    private String bucket;

    private String schemaTermsFile;

    /* (non-Javadoc)
     * @see io.cloudex.framework.Executable#run()
     */
    @Override
    public void run() throws IOException {

        Stopwatch stopwatch = Stopwatch.createStarted();

        log.info("START: ExtractAndCountTermsTask task" + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB");

        EcarfGoogleCloudService cloudService = (EcarfGoogleCloudService) this.getCloudService();

        TermCounter counter = null;

        if(StringUtils.isNoneBlank(this.schemaTermsFile)) {
            log.info("Downloading schema terms file: " + schemaTermsFile);
            Set<String> schemaTerms = cloudService.getSetFromCloudStorageFile(schemaTermsFile, bucket);
            counter = new TermCounter();
            counter.setTermsToCount(schemaTerms);
        }

        Set<String> filesSet = ObjectUtils.csvToSet(files);
        log.info("Processing files: " + filesSet);

        for(final String file: filesSet) {

            Stopwatch stopwatch1 = Stopwatch.createStarted();

            log.info("Downloading file: " + file + ", timer: 0s");
            String localFile = Utils.TEMP_FOLDER + file;
            cloudService.downloadObjectFromCloudStorage(file, localFile, sourceBucket);

            log.info("Processing file: " + localFile + ", timer: " + stopwatch1);
            NTripleGzipProcessor processor = new NTripleGzipProcessor(localFile);
            ExtractTermsCallback callback = new ExtractTermsCallback();
            callback.setCounter(counter);
            processor.read(callback);

            Set<String> terms = callback.getResources();
            terms.addAll(callback.getBlankNodes());

            log.info("Finished processing file: " + localFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch1);
            log.info("Number of unique URIs: " + callback.getResources().size());
            log.info("Number of blank nodes: " + callback.getBlankNodes().size());
            log.info("Number of literals: " + callback.getLiteralCount());

            String termsFile = Utils.TEMP_FOLDER + file + Constants.DOT_SER + Constants.GZIP_EXT;

            Utils.objectToFile(termsFile, terms, true);
            
            log.info("Serialized terms file: " + termsFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch1);

            cloudService.uploadFileToCloudStorage(termsFile, bucket);

            log.info("Uploaded terms file: " + termsFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch1);

        }

        // write term stats to file and upload
        if(counter != null) {

            log.info("Saving terms stats");
            String countStatsFile = Utils.TEMP_FOLDER + cloudService.getInstanceId() + Constants.DOT_JSON;
            FileUtils.objectToJsonFile(countStatsFile, counter.getCount());
            cloudService.uploadFileToCloudStorage(countStatsFile, bucket);
        }

        log.info("TIMER# All files are processed successfully, elapsed time: " + stopwatch);

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
     * @return the files
     */
    public String getFiles() {
        return files;
    }

    /**
     * @param files the files to set
     */
    public void setFiles(String files) {
        this.files = files;
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

}
