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
package io.ecarf.core.cloud.task.processor.old;

import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.utils.FileUtils;
import io.cloudex.framework.utils.ObjectUtils;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.cloud.task.processor.ProcessFilesForBigQuerySubTask;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public class ProcessLoadTask1Old extends CommonTask {
	
    private final static Log log = LogFactory.getLog(ProcessLoadTask1Old.class);

    private String bucket;

    private String schemaTermsFile;

    private String files;

    /* 
     * // TODO distinguish between files in cloud storage vs files downloaded from http or https url
	 * (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.Task#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("START: processing files for bigquery import");
		
		EcarfGoogleCloudService cloudService = (EcarfGoogleCloudService) this.getCloudService();

		//String bucket = metadata.getBucket();

		// get the schema terms if provided
		//String schemaTermsFile = metadata.getSchemaTermsFile();
		Set<String> schemaTerms = cloudService.getSetFromCloudStorageFile(schemaTermsFile, bucket);

		//Set<String> files = metadata.getFiles();
		Set<String> filesSet = ObjectUtils.csvToSet(files);
		log.info("Loading files: " + filesSet);
		Map<String, Integer> count = new HashMap<>();
		List<ProcessFilesForBigQuerySubTask> tasks = new ArrayList<>();
		int processors = Runtime.getRuntime().availableProcessors();

		for(final String file: filesSet) {
			
			TermCounter counter = null;
			
			if(schemaTerms != null) {
				counter = new TermCounter();
				counter.setTermsToCount(schemaTerms);
			}

			ProcessFilesForBigQuerySubTask task = new ProcessFilesForBigQuerySubTask(file, 
			        bucket, bucket, counter, null, false, false, this.getCloudService());
			tasks.add(task);

		}
		
		// check if we only have one file to process
		if(tasks.size() == 1) {
			
			TermCounter counter = tasks.get(0).call();
			if(counter != null) {
				count = counter.getCount();
			}
			
		} else if(processors == 1) {
			// only one process then process synchronously
			for(ProcessFilesForBigQuerySubTask task: tasks) {
				TermCounter counter = task.call();
				if(counter != null) {
					Utils.mergeCountMaps(count, counter.getCount());
				}
			}
			
		} else {
			
			// multiple cores
			ExecutorService executor = Utils.createFixedThreadPool(processors);
			
			try {
				
				List<Future<TermCounter>> results = executor.invokeAll(tasks);
				
				for (Future<TermCounter> result : results) {
					TermCounter counter = result.get();
					if(counter != null) {
						Utils.mergeCountMaps(count, counter.getCount());
					}
				}
				
			} catch(Exception e) {
				log.error("Failed to process multiple files", e);
				throw new IOException(e);
				
			} finally {
				executor.shutdown();
			}
		}
		// write term stats to file and upload
		if((count != null) && !count.isEmpty()) {
			log.info("Saving terms stats");
			String countStatsFile = Utils.TEMP_FOLDER + cloudService.getInstanceId() + Constants.DOT_JSON;
			FileUtils.objectToJsonFile(countStatsFile, count);

			cloudService.uploadFileToCloudStorage(countStatsFile, bucket);
		}

		log.info("FINISH: All files are processed and uploaded successfully");
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
	
}
