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
package io.ecarf.core.cloud;

import io.ecarf.core.utils.Callback;

import java.io.IOException;
import java.util.Map;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface CloudService {

	/**
	 * Perform initialization before
	 * this cloud service is used
	 * @return
	 * @throws IOException
	 */
	public VMMetaData inti() throws IOException;

	/**
	 * Create a bucket on the mass cloud storage
	 * @param bucket
	 * @param location
	 * @throws IOException
	 */
	public void createCloudStorageBucket(String bucket, String location) throws IOException;

	/**
	 * Upload the provided file into cloud storage
	 * @param filename
	 * @param bucket
	 * @param callback
	 * @throws IOException
	 */
	public void uploadFileToCloudStorage(String filename, String bucket, Callback callback) throws IOException;

	/**
	 * Download an object from cloud storage to a file
	 * @param object
	 * @param outFile
	 * @param bucket
	 * @param callback
	 * @throws IOException
	 */
	public void downloadObjectFromCloudStorage(String object, String outFile,
			String bucket, Callback callback) throws IOException;

	/**
	 * Convert the provided file to a format that can be imported to the Cloud Database
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public String prepareForCloudDatabaseImport(String filename) throws IOException;

	/**
	 * Update the meta data of the current instance
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	public void updateInstanceMetadata(String key, String value) throws IOException;

	/**
	 * Update the meta data of the current instance
	 * @param key
	 * @param value
	 * @param zone
	 * @param instanceId
	 * @throws IOException
	 */
	public void updateInstanceMetadata(Map<String, String> items, String zone, String instanceId) throws IOException;

	/**
	 * Update the meta data of the current instance
	 * @param items
	 * @throws IOException
	 */
	public void updateInstanceMetadata(Map<String, String> items) throws IOException;

	/**
	 * Get the meta data of the current instance, this will simply call the metadata server
	 * @return
	 * @throws IOException
	 */
	public VMMetaData getEcarfMetaData() throws IOException;

	/**
	 * Get the meta data for the provided instance id
	 * @param instanceId
	 * @param zoneId
	 * @return
	 * @throws IOException
	 */
	public VMMetaData getEcarfMetaData(String instanceId, String zoneId) throws IOException;



}
