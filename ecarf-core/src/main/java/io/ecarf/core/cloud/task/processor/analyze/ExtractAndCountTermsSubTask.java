package io.ecarf.core.cloud.task.processor.analyze;

import io.cloudex.framework.cloud.api.CloudService;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.compress.NTripleGzipProcessor;
import io.ecarf.core.compress.callback.ExtractTermsCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ExtractAndCountTermsSubTask implements Callable<TermCounter> {

    private final static Log log = LogFactory.getLog(ExtractAndCountTermsSubTask.class);

    private String file;
    private EcarfGoogleCloudService cloud;
    private TermCounter counter;
    private String bucket;
    private String sourceBucket;

    public ExtractAndCountTermsSubTask(String file, String bucket, String sourceBucket, TermCounter counter, CloudService cloud) {
        super();
        this.file = file;
        this.cloud = (EcarfGoogleCloudService) cloud;
        this.counter = counter;
        this.bucket = bucket;
        this.sourceBucket = sourceBucket;

    }

    @Override
    public TermCounter call() throws IOException {
        
        //"/Users/omerio/Ontologies/dbpedia/"

        String localFile = Utils.TEMP_FOLDER + file;

        log.info("START: Downloading file: " + file + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB");
        Stopwatch stopwatch = Stopwatch.createStarted();

        try {

            this.cloud.downloadObjectFromCloudStorage(file, localFile, this.sourceBucket);

            // all downloaded, carryon now, process the files

            log.info("Processing file: " + localFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);

            NTripleGzipProcessor processor = new NTripleGzipProcessor(localFile);
            ExtractTermsCallback callback = new ExtractTermsCallback();
            callback.setCounter(counter);
            processor.read(callback);
            
            Set<String> terms = callback.getResources();
            terms.addAll(callback.getBlankNodes());

            // once the processing is done then delete the local file
            //FileUtils.deleteFile(localFile);

            log.info("TIMER# Finished processing file: " + localFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);
            log.info("Number of unique URIs: " + callback.getResources().size());
            log.info("Number of blank nodes: " + callback.getBlankNodes().size());
            log.info("Number of literals: " + callback.getLiteralCount());
            
            callback = null;
            processor = null;
            
            String termsFile = Utils.TEMP_FOLDER + file + Constants.DOT_SER + Constants.GZIP_EXT;

            Utils.objectToFile(termsFile, terms, true);
            
            log.info("Serialized terms file: " + termsFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);

            this.cloud.uploadFileToCloudStorage(termsFile, bucket);

            log.info("Uploaded terms file: " + termsFile + ", memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);
            

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

        return counter;
    }

}