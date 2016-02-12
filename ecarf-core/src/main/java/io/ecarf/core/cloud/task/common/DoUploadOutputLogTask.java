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

import io.cloudex.framework.cloud.api.CloudService;
import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.utils.Config;
import io.ecarf.core.utils.Constants;

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

	private String bucket;
	

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("Uploading logs file");
		
		CloudService cloudService = this.getCloudService();
		
		String logFile = Config.getProperty(Constants.OUTPUT_FILE_KEY);
		String logFolder = Config.getProperty(Constants.OUTPUT_FOLDER_KEY);
		String instanceId = cloudService.getInstanceId();
		
		// get a random number just in case two instances have the same id, which is true for the coordinator
		int random = (int) (Math.random() * 1001);		
		
		String newLogFile = logFolder + Constants.OUTPUT + instanceId + "_" + random + Constants.DOT_LOG;
		log.info("Copying output log to " + newLogFile);
		FileUtils.copyFile(logFolder + logFile, newLogFile);
		
		// upload the file to cloud storage
		cloudService.uploadFileToCloudStorage(newLogFile, bucket);
		
		log.info("Successfully uploaded logs file");
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

}
