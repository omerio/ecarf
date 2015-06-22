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
import io.ecarf.core.utils.Config;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Given a Schema file on cloud storage, download it locally then count the relevant
 * terms then save the terms count in a schema_terms.json to cloud storage for use by 
 * the evms
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoUploadOutputLogTask extends CommonTask {
	
	private final static Log log = LogFactory.getLog(DoUploadOutputLogTask.class);

	public DoUploadOutputLogTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("Uploading logs file");
		
		String logFile = Config.getProperty(Constants.OUTPUT_FILE_KEY);
		String logFolder = Config.getProperty(Constants.OUTPUT_FOLDER_KEY);
		String bucket = metadata.getBucket();
		String instanceId = this.cloud.getInstanceId();
		
		String newLogFile = logFolder + Constants.OUTPUT + instanceId + Constants.DOT_LOG;
		log.info("Copying output log to " + newLogFile);
		Utils.copyFile(logFolder + logFile, newLogFile);
		
		// upload the file to cloud storage
		this.cloud.uploadFileToCloudStorage(newLogFile, bucket);
		
		log.info("Successfully uploaded logs file");
	}


}
