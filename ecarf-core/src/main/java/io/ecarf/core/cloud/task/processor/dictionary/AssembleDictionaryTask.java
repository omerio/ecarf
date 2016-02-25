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
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.compress.NxGzipProcessor;
import io.ecarf.core.compress.callback.DictionaryEncodeCallback;
import io.ecarf.core.compress.callback.ExtractTerms2PartCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.term.dictionary.ConcurrentDictionary;
import io.ecarf.core.term.dictionary.TermDictionary;
import io.ecarf.core.term.dictionary.TermDictionaryConcurrent;
import io.ecarf.core.utils.FilenameUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
    
    private String schemaBucket;
    
    private String schemaFile;
    
    private String termStatsFile;
    
    private String encodedSchemaFile;
    
    private String encodedTermStatsFile;
    
    private String dictionaryFile;

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
        
        // extract the terms and encode the schema if needed
        if(StringUtils.isNotBlank(this.schemaFile) && StringUtils.isNotBlank(this.schemaBucket))   {
            this.encodeSchema(dictionary);
        }
        
        // encode the term stats file is needed
        if(StringUtils.isNotBlank(this.termStatsFile) && StringUtils.isNotBlank(this.encodedTermStatsFile)) {
            this.encodeTermsStats(dictionary);
        }
        
        // if no name provided for the dictionary file then create a default
        if(StringUtils.isBlank(this.dictionaryFile)) {
            this.dictionaryFile = this.cloudService.getInstanceId() + '_' + FilenameUtils.getSerializedGZipedDictionaryFilename();
        }
        
        this.dictionaryFile = FilenameUtils.getLocalFilePath(this.dictionaryFile);
        
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
     * Encode the schema file and term stats
     * @param dictionary
     * @throws IOException 
     */
    private void encodeSchema(TermDictionary dictionary) throws IOException {


        log.info("Extracting schema terms and encoding from file: " + this.schemaFile);

        String localFile = Utils.TEMP_FOLDER + schemaFile;
        
        if(FilenameUtils.fileExists(localFile)) {
            log.info("Re-using local file: " + localFile);
            
        } else {
            
            this.cloudService.downloadObjectFromCloudStorage(schemaFile, localFile, this.schemaBucket);
        }

        // all downloaded, carryon now, process the files

        log.info("Processing schema file: " + localFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB");

        NxGzipProcessor processor = new NxGzipProcessor(localFile);
        ExtractTerms2PartCallback callback = new ExtractTerms2PartCallback();
        callback.setSplitLocation(-1);
        callback.setCounter(new TermCounter());
        processor.read(callback);

        Set<String> blankNodes = callback.getBlankNodes();
        Set<String> resources = callback.getResources();

        log.info("Number of schema resource URIs unique parts: " + resources.size());
        log.info("Number of schema blank nodes: " + blankNodes.size());
        log.info("Number of schema literals: " + callback.getLiteralCount());

        resources.addAll(blankNodes);

        for(String part: resources) {
            dictionary.add(part);
        }

        log.info("Schema terms added to the dictionary, final size: " + dictionary.size() + 
                " , memory usage: " + Utils.getMemoryUsageInGB() + "GB");

        // now encode the schema
        if(StringUtils.isNotBlank(this.encodedSchemaFile)) {
            
            log.info("Encoding the schema file to: " + this.encodedSchemaFile);
            
            String encLocalFile = Utils.TEMP_FOLDER + this.encodedSchemaFile;
            
            // NxGzip processor can write unzipped files
            processor = new NxGzipProcessor(localFile, encLocalFile);

            DictionaryEncodeCallback callback1 = new DictionaryEncodeCallback();

            callback1.setDictionary(dictionary);

            processor.process(callback1);
           
            // upload the file
            this.getCloudService().uploadFileToCloudStorage(encLocalFile, targetBucket);
            
        }


    }
    
    /**
     * Encode the terms stats file
     * @param dictionary
     * @throws IOException
     */
    private void encodeTermsStats(TermDictionary dictionary) throws IOException {
        
        log.info("Encoding term stats map from: " + this.termStatsFile);
        
        String localFile = Utils.TEMP_FOLDER + termStatsFile;
        
        this.cloudService.downloadObjectFromCloudStorage(termStatsFile, localFile, this.bucket);

        // all downloaded, carryon now, process the files

        log.info("Processing terms stats file: " + localFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB");

        // convert from JSON
        Map<String, Long> termStats = FileUtils.jsonFileToMap(localFile);

        Map<String, Long> encTermStats = new HashMap<>();

        for(Entry<String, Long> term: termStats.entrySet()) {
            String key = term.getKey();
            Long value = term.getValue();
            
            long enc = dictionary.encode(key);
            
            encTermStats.put(Long.toString(enc), value);

        }
        
        // if we have a term stats file set then serialize the term stats map to cloud storage
        
        log.info("Serializing term stats to file: " + this.encodedTermStatsFile);
        localFile = Utils.TEMP_FOLDER + this.encodedTermStatsFile;

        // save to file
        FileUtils.objectToJsonFile(localFile, encTermStats);

        // upload the file to cloud storage
        this.getCloudService().uploadFileToCloudStorage(localFile, targetBucket);
        
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

    /**
     * @return the schemaBucket
     */
    public String getSchemaBucket() {
        return schemaBucket;
    }

    /**
     * @param schemaBucket the schemaBucket to set
     */
    public void setSchemaBucket(String schemaBucket) {
        this.schemaBucket = schemaBucket;
    }

    /**
     * @return the schemaFile
     */
    public String getSchemaFile() {
        return schemaFile;
    }

    /**
     * @param schemaFile the schemaFile to set
     */
    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }

    /**
     * @return the termStatsFile
     */
    public String getTermStatsFile() {
        return termStatsFile;
    }

    /**
     * @param termStatsFile the termStatsFile to set
     */
    public void setTermStatsFile(String termStatsFile) {
        this.termStatsFile = termStatsFile;
    }

    /**
     * @return the encodedSchemaFile
     */
    public String getEncodedSchemaFile() {
        return encodedSchemaFile;
    }

    /**
     * @param encodedSchemaFile the encodedSchemaFile to set
     */
    public void setEncodedSchemaFile(String encodedSchemaFile) {
        this.encodedSchemaFile = encodedSchemaFile;
    }

    /**
     * @return the encodedTermStatsFile
     */
    public String getEncodedTermStatsFile() {
        return encodedTermStatsFile;
    }

    /**
     * @param encodedTermStatsFile the encodedTermStatsFile to set
     */
    public void setEncodedTermStatsFile(String encodedTermStatsFile) {
        this.encodedTermStatsFile = encodedTermStatsFile;
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
