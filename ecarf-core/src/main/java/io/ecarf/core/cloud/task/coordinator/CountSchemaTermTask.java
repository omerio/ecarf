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

import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.term.TermUtils;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
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
public class CountSchemaTermTask extends CommonTask {
	
	private final static Log log = LogFactory.getLog(CountSchemaTermTask.class);
	
	private String bucket;
	
	private String schemaFile;


	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("Processing schema terms");
		
		//String schemaFile = metadata.getValue(EcarfMetaData.ECARF_SCHEMA);
		//String bucket = metadata.getBucket();
		
		String localSchemaFile = Utils.TEMP_FOLDER + schemaFile;
		// download the file from the cloud storage
		this.getCloudService().downloadObjectFromCloudStorage(schemaFile, localSchemaFile, bucket);
		
		// uncompress if compressed
		if(GzipUtils.isCompressedFilename(schemaFile)) {
			localSchemaFile = GzipUtils.getUncompressedFilename(localSchemaFile);
		}
		
		Set<String> terms = TermUtils.getRelevantSchemaTerms(localSchemaFile, TermUtils.RDFS_TBOX);
		
		log.info("Total relevant schema terms: " + terms.size());
		
		String schemaTermsFile = Utils.TEMP_FOLDER + Constants.SCHEMA_TERMS_JSON;
		
		// save to file
		FileUtils.objectToJsonFile(schemaTermsFile, terms);
		
		// upload the file to cloud storage
		this.getCloudService().uploadFileToCloudStorage(schemaTermsFile, bucket);
		
		// add value to the output
		this.addOutput("schemaTermsFile", Constants.SCHEMA_TERMS_JSON);
		
		log.info("Successfully processed schema terms");
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
     * @return the schemaFile
     */
    public String getSchemaFile() {
        return schemaFile;
    }


    /**
     * @param schemaFile the schemaFile to set
     */
    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }


}
