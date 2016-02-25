package io.ecarf.core.cloud.task.processor;

import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.compress.NxGzipProcessor;
import io.ecarf.core.compress.callback.DictionaryEncodeCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.term.dictionary.TermDictionary;
import io.ecarf.core.utils.FilenameUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ProcessFilesForBigQuerySubTask implements Callable<TermCounter> {
	
	private final static Log log = LogFactory.getLog(ProcessFilesForBigQuerySubTask.class);

	private String file;
	private EcarfGoogleCloudService cloud;
	private TermCounter counter;
	private String bucket;
	private String sourceBucket;
	private boolean countOnly;
	private boolean encode;
	private TermDictionary dictionary;

	public ProcessFilesForBigQuerySubTask(String file, String bucket, String sourceBucket, TermCounter counter, 
	        TermDictionary dictionary, boolean countOnly, boolean encode, CloudService cloud) {
		super();
		this.file = file;
		this.cloud = (EcarfGoogleCloudService) cloud;
		this.counter = counter;
		this.bucket = bucket;
		this.countOnly = countOnly;
		this.encode = encode;
		this.sourceBucket = sourceBucket;
		this.dictionary = dictionary;
	}

	@Override
	public TermCounter call() throws IOException {
	    
	    log.info("Processing file for BigQuery import: " + file);

		String localFile = Utils.TEMP_FOLDER + file;

		if(FilenameUtils.fileExists(localFile)) {
            log.info("Re-using local file: " + localFile);
            
        } else {
            
            this.cloud.downloadObjectFromCloudStorage(file, localFile, sourceBucket);
        }

		// all downloaded, carryon now, process the files
		log.info("Processing file: " + localFile + ", countOnly = " + countOnly + ", encode = " + encode + 
		        ", memory usage: " + Utils.getMemoryUsageInGB() + "GB");
		
		String outFile = "";
		
		if(encode) {
		    
		    NxGzipProcessor processor = new NxGzipProcessor(localFile);

		    DictionaryEncodeCallback callback = new DictionaryEncodeCallback();
		    // set the dictionary
		    callback.setDictionary(dictionary);
	        callback.setCounter(counter);
	        
	        outFile = processor.process(callback);
		    
		} else {
		    
		    outFile = this.cloud.prepareForBigQueryImport(localFile, counter, countOnly);
		}

		// once the processing is done then delete the local file
		FileUtils.deleteFile(localFile);

		// if we are not just counting then upload the output files
		if(!countOnly) {
		    // now upload the files again
		    log.info("Uploading file: " + outFile);
		    this.cloud.uploadFileToCloudStorage(outFile, bucket);

		    // now delete all the locally processed files
		    FileUtils.deleteFile(outFile);
		}

		return counter;
	}


}