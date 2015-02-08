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
package io.ecarf.core.cloud.task.impl.load;

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.storage.StorageObject;
import io.ecarf.core.cloud.task.CommonTask;
import io.ecarf.core.utils.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Read a list of files from cloud storage and import them into big data table
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoLoadTask extends CommonTask {
	

	public DoLoadTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("Processing import into big data table");
		
		String bucket = this.input.getBucket();
		
		String table = this.input.getTable();
		
		List<StorageObject> objects = this.cloud.listCloudStorageObjects(bucket);
		
		List<String> files = new ArrayList<>();
		
		for(StorageObject object: objects) {
			String filename = object.getName();
			if(filename.endsWith(Constants.PROCESSED_FILES)) {
				files.add(object.getUri());
			} else {
				log.warn("Skipping file: " + filename);
			}
		}
		
		String jobId = this.cloud.loadCloudStorageFilesIntoBigData(files, table, true);
		
		log.info("Successfully imported data into big table, completed jodId: " + jobId);
	}
	

}
