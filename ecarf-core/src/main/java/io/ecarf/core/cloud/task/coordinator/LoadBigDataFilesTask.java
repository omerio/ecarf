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
package io.ecarf.core.cloud.task.coordinator;

import io.cloudex.framework.cloud.entities.BigDataTable;
import io.cloudex.framework.cloud.entities.StorageObject;
import io.cloudex.framework.task.CommonTask;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.TableUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Read a list of files from cloud storage and import them into big data table
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class LoadBigDataFilesTask extends CommonTask {
	
	private final static Log log = LogFactory.getLog(LoadBigDataFilesTask.class);
	
	private String bucket;
	
	private String table;
	
	private String encode;

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("Processing import into big data table: " + table + ", with encode: " + encode);
		
		List<StorageObject> objects = this.getCloudService().listCloudStorageObjects(bucket);
		
		List<String> files = new ArrayList<>();
		
		for(StorageObject object: objects) {
			String filename = object.getName();
			if(filename.endsWith(Constants.PROCESSED_FILES)) {
				files.add(object.getUri());
			} else {
				log.warn("Skipping file: " + filename);
			}
		}
		
		BigDataTable bigDataTable;
		
		if(Boolean.valueOf(encode)) {
		    
		    bigDataTable = TableUtils.getBigQueryEncodedTripleTable(table);
		    
		} else {
		    
		    bigDataTable = TableUtils.getBigQueryTripleTable(table);
		}
				
		String jobId = this.getCloudService().loadCloudStorageFilesIntoBigData(files, bigDataTable, true);
		
		log.info("Successfully imported data into big table, completed jodId: " + jobId);
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
     * @return the table
     */
    public String getTable() {
        return table;
    }

    /**
     * @param table the table to set
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * @return the encode
     */
    public String getEncode() {
        return encode;
    }

    /**
     * @param encode the encode to set
     */
    public void setEncode(String encode) {
        this.encode = encode;
    }
	

}
