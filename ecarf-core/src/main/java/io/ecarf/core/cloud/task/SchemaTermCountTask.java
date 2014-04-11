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
import io.ecarf.core.term.TermUtils;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.compress.compressors.gzip.GzipUtils;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class SchemaTermCountTask extends CommonTask {

	public SchemaTermCountTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("Processing schema terms");
		
		String schemaFile = metadata.getValue(VMMetaData.ECARF_SCHEMA);
		String bucket = metadata.getBucket();
		
		String localSchemaFile = Utils.TEMP_FOLDER + schemaFile;
		// download the file from the cloud storage
		this.cloud.downloadObjectFromCloudStorage(schemaFile, localSchemaFile, bucket);
		
		// uncompress if compressed
		if(GzipUtils.isCompressedFilename(schemaFile)) {
			localSchemaFile = GzipUtils.getUncompressedFilename(localSchemaFile);
		}
		
		Set<String> terms = TermUtils.getRelevantSchemaTerms(localSchemaFile, TermUtils.RDFS_TBOX);
		
		log.info("Total relevant schema terms: " + terms.size());
		
		String schemaTermsFile = Utils.TEMP_FOLDER + Constants.SCHEMA_TERMS_JSON;
		
		// save to file
		Utils.objectToJsonFile(schemaTermsFile, terms);
		
		// upload the file to cloud storage
		this.cloud.uploadFileToCloudStorage(schemaTermsFile, bucket);
		
		log.info("Successfully processed schema terms");
	}

}
