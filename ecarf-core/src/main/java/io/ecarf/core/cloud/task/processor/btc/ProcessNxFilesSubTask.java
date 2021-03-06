package io.ecarf.core.cloud.task.processor.btc;

import io.cloudex.framework.cloud.api.CloudService;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.compress.NxGzipProcessor;
import io.ecarf.core.compress.callback.ExtractTerms2PartCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.utils.FilenameUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * 1- Download a gziped N-Triple file from cloud storage
 * 2- Process the file in memory, extract and blank nodes and build the resources tree and count the literals
 * 3- Serialize the blank nodes and resource tree and upload to cloud storage
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ProcessNxFilesSubTask implements Callable<Void> {

    private final static Log log = LogFactory.getLog(ProcessNxFilesSubTask.class);

    private String file;
    private EcarfGoogleCloudService cloud;

    private String bucket;
    private String sourceBucket;

    public ProcessNxFilesSubTask(String file, String bucket, String sourceBucket, CloudService cloud) {
        super();
        this.file = file;
        this.cloud = (EcarfGoogleCloudService) cloud;

        this.bucket = bucket;
        this.sourceBucket = sourceBucket;

    }

    @Override
    public Void call() throws IOException {
        
        String localFile = FilenameUtils.getLocalFilePath(file);

        log.info("START: Downloading file: " + file + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB");
        Stopwatch stopwatch = Stopwatch.createStarted();

        try {

            this.cloud.downloadObjectFromCloudStorage(file, localFile, this.sourceBucket);

            // all downloaded, carryon now, process the files

            log.info("Processing file: " + localFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);

            Stopwatch stopwatch1 = Stopwatch.createStarted();
            NxGzipProcessor processor = new NxGzipProcessor(localFile);
            StatsExtractSchemaCallback callback = new StatsExtractSchemaCallback();
            
            processor.read(callback);
            
            Set<String> blankNodes = null;//callback.getBlankNodes();
            Set<String> resources = null;//callback.getResources();

            stopwatch1.stop();
            // once the processing is done then delete the local file
            //FileUtils.deleteFile(localFile);

            log.info("TIMER# Finished processing file: " + localFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", info: " + file + "," + stopwatch1);
            log.info("Number of resource URIs unique parts: " + file + "," + resources.size());
            log.info("Number of blank nodes: " + file + ",");// + callback.getBlankNodes().size());
            log.info("Number of literals: " + file + ",");// + callback.getLiteralCount());
            
            callback = null;
            processor = null;
            
            // upload the terms tree
            String termsFile = FilenameUtils.getLocalSerializedGZipedFilePath(file, false);
            this.serializeAndUploadObject(termsFile, resources, stopwatch);
            
            // upload the blank nodes
            if(!blankNodes.isEmpty()) {
                String blankNodesFile = FilenameUtils.getLocalSerializedGZipedBNFilePath(file, false);
                this.serializeAndUploadObject(blankNodesFile, blankNodes, stopwatch);
            }
            
            blankNodes = null;
            resources = null;

        } catch(Exception e) {
            // because this sub task is run in an executor the exception will be stored and thrown in the
            // future, but we want to know about it now, so log it
            log.error("Failed to download or process file: " + file, e);

            if(e instanceof IOException) {
                throw e;
            } else {
                throw new IOException(e);
            }
            
        } catch(Error e) {
            
            log.fatal("JVM Died: ", e);
            throw e;
        }

        return null;
    }
    
    /**
     * Serialize the provided object and upload the file to cloud storage
     * @param file
     * @param object
     * @param stopwatch
     * @throws IOException
     */
    private void serializeAndUploadObject(String file, Object object, Stopwatch stopwatch) throws IOException {
        Utils.objectToFile(file, object, true, false);
        
        log.info("Serialized file: " + file + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);

        this.cloud.uploadFileToCloudStorage(file, bucket);

        log.info("Uploaded file: " + file + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);
    }

}