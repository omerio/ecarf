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


package io.ecarf.core.cloud.task.processor.dictionary;

import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.term.dictionary.TermDictionary;
import io.ecarf.core.utils.FilenameUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * The underlying sub task that is assembling the dictionary
 * @author Omer Dawelbeit (omerio)
 *
 */
public class AssembleDictionarySubTask implements Callable<Void> {

    private final static Log log = LogFactory.getLog(AssembleDictionarySubTask.class);

    private TermDictionary dictionary;
    private EcarfGoogleCloudService cloud;
    private String bucket;
    private List<Item> files;

    /**
     * @param dictionary
     * @param cloud
     * @param bucket
     * @param files
     */
    public AssembleDictionarySubTask(TermDictionary dictionary,
            EcarfGoogleCloudService cloud, String bucket, List<Item> files) {
        super();
        this.dictionary = dictionary;
        this.cloud = cloud;
        this.bucket = bucket;
        this.files = files;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Void call() throws Exception {

        for(Item item: this.files) {
            
            String file = item.getKey();

            String localFile = FilenameUtils.getLocalFilePath(file);

            log.info("START: Downloading file: " + file + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB");
            Stopwatch stopwatch = Stopwatch.createStarted();

            try {

                this.cloud.downloadObjectFromCloudStorage(file, localFile, this.bucket);

                // all downloaded, carryon now, process the files
                log.info("Processing file: " + localFile + ", dictionary items: " + dictionary.size() + 
                        ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);
                
                Set<String> parts = Utils.objectFromFile(localFile, HashSet.class, true, false);
                
                log.info("Processing: " + parts.size() + " term parts , memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);

                for(String part: parts) {
                    dictionary.add(part);
                }
                
                // immediately release parts
                parts = null;

                log.info("TIMER# Finished processing file: " + localFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);
                log.info("Dictionary size: " + dictionary.size());
                
                FileUtils.deleteFile(localFile);

            } catch(Exception e) {
                // because this sub task is run in an executor the exception will be stored and thrown in the
                // future, but we want to know about it now, so log it
                log.error("Failed to download or process file: " + file, e);

                if(e instanceof IOException) {
                    throw e;
                } else {
                    throw new IOException(e);
                }
            } 
        }

        return null;
    }

}
