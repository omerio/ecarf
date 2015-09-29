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

import io.cloudex.framework.cloud.entities.StorageObject;
import io.cloudex.framework.partition.builtin.BinPackingPartition;
import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.partition.entities.Partition;
import io.cloudex.framework.task.CommonTask;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.term.dictionary.ConcurrentDictionary;
import io.ecarf.core.term.dictionary.TermDictionary;
import io.ecarf.core.term.dictionary.TermDictionaryConcurrent;
import io.ecarf.core.utils.FilenameUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class AssembleDictionaryTask extends CommonTask {
    
    private final static Log log = LogFactory.getLog(AssembleDictionaryTask.class);

    private String bucket;
    
    private String targetBucket;

    /* (non-Javadoc)
     * @see io.cloudex.framework.Executable#run()
     */
    @Override
    public void run() throws IOException {
        
        log.info("Assembling dictionary, memory usage: " + Utils.getMemoryUsageInGB() + "GB");
        
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        List<StorageObject> objects = this.cloudService.listCloudStorageObjects(bucket);
        
        //Set<String> files = new HashSet<>();
        
        List<Item> items = new ArrayList<>();
         
        for(StorageObject object: objects) {
            
            String filename = object.getName();
            
            if(filename.endsWith(FilenameUtils.KRYO_SERIALIZED_EXT)) {
                //files.add(filename);
                items.add(new Item(filename, object.getSize().longValue()));
            }
        }
        
        log.info("Found " + items.size() + ", serialized files");
        
        int processors = Runtime.getRuntime().availableProcessors();
        
        BinPackingPartition function = new BinPackingPartition(items);
        function.setMaxBinItems((long) processors);
        List<Partition> partitions = function.partition();
        
        TermDictionary dictionary = TermDictionary.populateRDFOWLData(new TermDictionaryConcurrent());
        
        List<Callable<Void>> tasks = getSubTasks(partitions, dictionary);

        try {

            // check if we only have one file to process
            if(tasks.size() == 1) {

                tasks.get(0).call();

            } else if(processors == 1) {
                // only one process then process synchronously
               
                for(Callable<Void> task: tasks) {
                   task.call();
                }

            } else {

                // multiple cores
                ExecutorService executor = Utils.createFixedThreadPool(processors);

                try {

                    executor.invokeAll(tasks);
                    
                } finally {
                    executor.shutdown();
                }
            }
            
            tasks = null;
            
        } catch(Exception e) {
            log.error("Failed to process multiple files", e);
            throw new IOException(e);

        }
        
        int dicSize = dictionary.size();
        
        log.info("Successfully assembled dictionary with size: " + dicSize + ", max resourceId: " + dictionary.getLargestResourceId() +
                ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);
        
        String dictionaryFile = FilenameUtils.getLocalFilePath(this.cloudService.getInstanceId()
                + '_' + FilenameUtils.getSerializedGZipedDictionaryFilename());
        
        dictionary = ((ConcurrentDictionary) dictionary).getNonConcurrentDictionary();
        
        log.info("Successfully created non concurrent dictionary for serialization, memory usage: " + 
                Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);
        
        dictionary.toFile(dictionaryFile, true);
        
        dictionary = null;
        
        log.info("Successfully serialized dictionary with size: " + dicSize + 
                ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);
        
        if(StringUtils.isBlank(this.targetBucket)) {
            this.targetBucket = bucket;
        }
        
        this.cloudService.uploadFileToCloudStorage(dictionaryFile, this.targetBucket);
        
        log.info("Successfully assembled, serialized and uploaded dictionary, memory usage: " + 
                Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);

    }
    
    /**
     * Get a list of callable tasks
     * @param files
     * @return
     */
    public List<Callable<Void>> getSubTasks(List<Partition> partitions, TermDictionary dictionary) {
        List<Callable<Void>> tasks = new ArrayList<>();
        
        for(Partition partition: partitions) {

            AssembleDictionarySubTask task = 
                    new AssembleDictionarySubTask(dictionary, (EcarfGoogleCloudService) this.cloudService, this.bucket, partition.getItems());

            tasks.add(task);

        }

        return tasks;
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
     * @return the targetBucket
     */
    public String getTargetBucket() {
        return targetBucket;
    }

    /**
     * @param targetBucket the targetBucket to set
     */
    public void setTargetBucket(String targetBucket) {
        this.targetBucket = targetBucket;
    }

}
