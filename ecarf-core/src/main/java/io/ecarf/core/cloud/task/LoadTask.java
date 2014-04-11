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
package io.ecarf.core.cloud.task;

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.utils.Callback;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * read the files from http:// or from gs://
 * download files locally (gziped)
 * read through the files counting the relevant terms and rewriting 
 * into bigquery format (comma separated)
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class LoadTask extends CommonTask {

	
	public LoadTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* 
	 * // TODO disinguish between files in cloud storage vs files downloaded from http or https url
	 * (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.Task#run()
	 */
	@Override
	public void run() throws IOException {
		
		Set<String> files = metadata.getFiles();
		log.info("Loading files: " + files);
		String bucket = metadata.getBucket();
		
		final Set<String> localFiles = new HashSet<>();
		Set<String> localProcessedFiles = new HashSet<>();
		final Set<String> uploadedFiles = new HashSet<>();
		
		for(final String file: files) {
			
			final String localFile = Utils.TEMP_FOLDER + file;
			log.info("Downloading file: " + file);
			this.cloud.downloadObjectFromCloudStorage(file, 
					localFile, bucket, new Callback() {
				
				@Override
				public void execute() {
					log.info("Download complete, file saved to: " + localFile);
					// add the file to the list of downloaded files
					localFiles.add(localFile);
				}
			});
		}
		
		this.waitForEquality(files.size(), localFiles.size());
		
		// all downloaded, carryon now, process the files
		// TODO add term analysis
		for(String file: localFiles) {
			log.info("Processing file: " + file);
			String outFile = this.cloud.prepareForCloudDatabaseImport(file);
			localProcessedFiles.add(outFile);
		}
		
		// now upload the files again
		for(final String file: localProcessedFiles) {
			log.info("Uploading file: " + file);
			this.cloud.uploadFileToCloudStorage(file, bucket, new Callback() {
				
				@Override
				public void execute() {
					log.info("Upload complete of file: " + file);
					// add the file to the list of downloaded files
					uploadedFiles.add(file);
				}
			});
		}
		
		this.waitForEquality(localProcessedFiles.size(), uploadedFiles.size());
		log.info("All files are processed and uploaded successfully");
	}

}
