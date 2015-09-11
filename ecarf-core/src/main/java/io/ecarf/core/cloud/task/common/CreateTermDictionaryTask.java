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


package io.ecarf.core.cloud.task.common;

import io.cloudex.cloud.impl.google.compute.GoogleMetaData;
import io.cloudex.framework.task.CommonTask;
import io.ecarf.core.compress.NTripleGzipProcessor;
import io.ecarf.core.compress.callback.ExtractTermsCallback;
import io.ecarf.core.term.TermDictionary;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class CreateTermDictionaryTask extends CommonTask {

    private final static Log log = LogFactory.getLog(CreateTermDictionaryTask.class);

    private String bucket;
    
    private String sourceBucket;

    private Collection<String> processors;

    private String schemaFile;

    private Set<String> allTerms = new HashSet<>();

    /* (non-Javadoc)
     * @see io.cloudex.framework.Executable#run()
     */
    @SuppressWarnings("unchecked")
    @Override
    public void run() throws IOException {

        log.info("Creating terms dictionary.");
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        if(StringUtils.isBlank(sourceBucket)) {
            log.warn("sourceBucket is empty, using bucket: " + bucket);
            this.sourceBucket = bucket;
        }

        // 1- Get and combine the terms from all the nodes
        // 2- Get all the terms from the schema file
        // 3- Create a dictionary bootstrapped with RDF & OWL URIs
        // 4- Add all to the dictionary, gzip and upload to cloud storage
        // 5- encode the schema & upload to cloud storage
        // 6- encode the relevant schema terms and upload to cloud storage

        // 1- Get and combine the terms from all the nodes
        for(String instanceId: this.processors) {   

            String termsFile = Constants.NODE_TERMS + 
                    instanceId + Constants.DOT_SER + Constants.GZIP_EXT;

            String localTermsFile = Utils.TEMP_FOLDER + termsFile;

            log.info("Downloading processor terms file: " + termsFile + ", timer: " + stopwatch);

            try {
                this.getCloudService().downloadObjectFromCloudStorage(termsFile, localTermsFile, bucket);

                log.info("Uncompressing processor terms file: " + termsFile + ", timer: " + stopwatch);
                String uncompressedFile = Utils.unCompressFile(localTermsFile);

                log.info("De-serializing processor terms file: " + termsFile + ", timer: " + stopwatch);
                Set<String> nodeTerms = Utils.objectFromFile(uncompressedFile, HashSet.class);

                if(nodeTerms != null) {
                    this.allTerms.addAll(nodeTerms);
                }

            } catch(IOException e) {
                // a file not found means the evm didn't find any schema terms so didn't generate any stats
                log.error("failed to download file: " + localTermsFile, e);
                if(!(e.getMessage().indexOf(GoogleMetaData.NOT_FOUND) >= 0)) {
                    throw e;
                }
            } catch (ClassNotFoundException e) {
                log.error("failed to de-serialize file: " + localTermsFile, e);
                throw new IOException(e);
            }
        }
        
        // 2- Get all the terms from the schema file
        String localSchemaFile = Utils.TEMP_FOLDER + schemaFile;
        
        Path path = Paths.get(localSchemaFile);
        
        log.info("Getting terms from schema file: " + localSchemaFile + ", timer: " + stopwatch);

        if (!Files.exists(path)) {
            // download the file from the cloud storage
            this.getCloudService().downloadObjectFromCloudStorage(schemaFile, localSchemaFile, sourceBucket);
        
        } else {
            log.info("Schema file exists locally.");
        }

        NTripleGzipProcessor processor = new NTripleGzipProcessor(localSchemaFile);
        ExtractTermsCallback callback = new ExtractTermsCallback();

        processor.read(callback);

        this.allTerms.addAll(callback.getResources());
        this.allTerms.addAll(callback.getBlankNodes());
        
        log.info("TIMER# Finished processing schema file: " + localSchemaFile + ", timer: " + stopwatch);
        log.info("Number of unique URIs: " + callback.getResources().size());
        log.info("Number of blank nodes: " + callback.getBlankNodes().size());
        log.info("Number of literals: " + callback.getLiteralCount());
        
        
        // 3- Create a dictionary bootstrapped with RDF & OWL URIs
        log.info("Creating terms dictionary, timer: " + stopwatch);
        TermDictionary dictionary = TermDictionary.createRDFOWLDictionary();
        
        log.info("Removing RDF & OWL terms from all terms, timer: " + stopwatch);
        // should be faster to remove these terms than looping through all terms and checking
        for(String rdfOwlTerm: SchemaURIType.RDF_OWL_TERMS) {
            this.allTerms.remove(rdfOwlTerm);
        }
        
        
        // 4- Add all to the dictionary, gzip and upload to cloud storage
        log.info("Adding terms to dictionary, timer: " + stopwatch);
        for(String term: this.allTerms) {
            dictionary.add(term);
        }
        
        log.info("Serializing dictionary, timer: " + stopwatch);
        String dictionaryFile = Utils.TEMP_FOLDER + Constants.DICTIONARY_SER;
        String savedDictionaryFile = dictionary.toFile(dictionaryFile, true);
        
        log.info("Uploading dictionary to cloud storage, timer: " + stopwatch);
        // upload the file to cloud storage
        this.cloudService.uploadFileToCloudStorage(savedDictionaryFile, bucket);
        
        this.addOutput("dictionary", Constants.DICTIONARY_SER + Constants.GZIP_EXT);
        log.info("TIMER# successfully created terms dictionary in: " + stopwatch);


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
     * @return the processors
     */
    public Collection<String> getProcessors() {
        return processors;
    }

    /**
     * @param processors the processors to set
     */
    public void setProcessors(Collection<String> processors) {
        this.processors = processors;
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
