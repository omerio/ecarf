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
package io.ecarf.core.cloud.task.impl;

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.task.CommonTask;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * EVM Task to load the provided cloud files into the big data cloud storage
 * Does also analyse the terms as they are being processed
 * read the files from http:// or from gs://
 * download files locally (gziped)
 * read through the files counting the relevant terms and rewriting 
 * into bigquery format (comma separated)
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ProcessLoadTask extends CommonTask {
	
	private final static Log log = LogFactory.getLog(ProcessLoadTask.class);


	public ProcessLoadTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* 
	 * // TODO distinguish between files in cloud storage vs files downloaded from http or https url
	 * (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.Task#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("START: processing files for bigquery import");

		String bucket = metadata.getBucket();

		// get the schema terms if provided
		String schemaTermsFile = metadata.getSchemaTermsFile();
		TermCounter counter = null;

		if(StringUtils.isNoneBlank(schemaTermsFile)) {
			String localSchemaTermsFile = Utils.TEMP_FOLDER + schemaTermsFile;
			this.cloud.downloadObjectFromCloudStorage(schemaTermsFile, localSchemaTermsFile, bucket);

			// convert from JSON
			Set<String> schemaTerms = Utils.jsonFileToSet(localSchemaTermsFile);
			counter = new TermCounter();
			counter.setTermsToCount(schemaTerms);
		} 

		Set<String> files = metadata.getFiles();
		log.info("Loading files: " + files);

		for(final String file: files) {

			String localFile = Utils.TEMP_FOLDER + file;

			log.info("Downloading file: " + file);

			this.cloud.downloadObjectFromCloudStorage(file, localFile, bucket);

			// all downloaded, carryon now, process the files

			log.info("Processing file: " + localFile);
			String outFile = this.cloud.prepareForCloudDatabaseImport(localFile, counter);

			// once the processing is done then delete the local file
			Utils.deleteFile(localFile);

			// now upload the files again

			log.info("Uploading file: " + outFile);
			this.cloud.uploadFileToCloudStorage(outFile, bucket);

			// now delete all the locally processed files
			Utils.deleteFile(outFile);

		}

		// write term stats to file and upload
		if(counter != null) {
			log.info("Saving terms stats");
			String countStatsFile = Utils.TEMP_FOLDER + this.cloud.getInstanceId() + Constants.DOT_JSON;
			Utils.objectToJsonFile(countStatsFile, counter.getCount());

			this.cloud.uploadFileToCloudStorage(countStatsFile, bucket);
		}

		log.info("FINISH: All files are processed and uploaded successfully");
	}

}
