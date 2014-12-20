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

import io.ecarf.core.cloud.storage.StorageObject;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.utils.Callback;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;

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
	 * Upload a file to cloud storage and block until it's uploaded
	 * @param filename
	 * @param bucket
	 * @throws IOException
	 */
	public void uploadFileToCloudStorage(String filename, String bucket) throws IOException;

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
	 * Download an object from cloud storage to a file, this method will block until the file is downloaded
	 * @param object
	 * @param outFile
	 * @param bucket
	 * @throws IOException
	 */
	public void downloadObjectFromCloudStorage(String object, String outFile,
			String bucket) throws IOException;
	
	/**
	 * List all the objects in the provided cloud storage bucket
	 * @param bucket
	 * @return
	 * @throws IOException
	 */
	public List<StorageObject> listCloudStorageObjects(String bucket) throws IOException;

	/**
	 * Convert the provided file to a format that can be imported to the Cloud Database
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public String prepareForCloudDatabaseImport(String filename) throws IOException;
	
	/**
	 * Convert the provided file to a format that can be imported to the Cloud Database, 
	 * this method will also count the terms using the provided counter
	 * 
	 * @param filename
	 * @param counter
	 * @return
	 * @throws IOException
	 */
	public String prepareForCloudDatabaseImport(String filename, TermCounter counter) throws IOException;
	
	/**
	 * Update the meta data of the current instance
	 * @param key
	 * @param value
	 * @param zone
	 * @param instanceId
	 * @throws IOException
	 */
	public void updateInstanceMetadata(VMMetaData metaData, String zone, String instanceId, boolean block) throws IOException;

	/**
	 * Update the meta data of the current instance
	 * @param items
	 * @throws IOException
	 */
	public void updateInstanceMetadata(VMMetaData metaData) throws IOException;

	/**
	 * Get the meta data of the current instance, this will simply call the metadata server
	 * @return
	 * @throws IOException
	 */
	public VMMetaData getEcarfMetaData(boolean waitForChange) throws IOException;

	/**
	 * Get the meta data for the provided instance id
	 * @param instanceId
	 * @param zoneId
	 * @return
	 * @throws IOException
	 */
	public VMMetaData getEcarfMetaData(String instanceId, String zoneId) throws IOException;

	/**
	 * Create VM instances, optionally block until all are created. If any fails then the returned flag is false
	 * @param configs
	 * @param block
	 * @return
	 * @throws IOException
	 */
	public boolean startInstance(List<VMConfig> configs, boolean block) throws IOException;

	/**
	 * Delete the VMs provided in this config
	 * @param configs
	 * @throws IOException
	 */
	public void shutdownInstance(List<VMConfig> configs) throws IOException;

	/**
	 * Delete the currently running vm, i.e. self terminate
	 * @throws IOException
	 */
	public void shutdownInstance() throws IOException;

	/**
	 * Get the id of the current instance
	 * @return
	 */
	public String getInstanceId();

	/*
	 * Load a list of cloud storage files into a big data table
	 * @param files - The source URIs must be fully-qualified, in the format gs://<bucket>/<object>.
	 * @param table
	 * @param createTable
	 * @return
	 * @throws IOException
	 */
	public String loadCloudStorageFilesIntoBigData(List<String> files, String table, boolean createTable) throws IOException;

	/**
	 * Load a list of local files into a big data table
	 * @param files
	 * @param table
	 * @param createTable
	 * @return
	 * @throws IOException
	 */
	public List<String> loadLocalFilesIntoBigData(List<String> files, String table, boolean createTable) throws IOException;

	/**
	 * Creates an asynchronous Query Job for a particular query on a dataset
	 *
	 * @param querySql  the actual query string
	 * @return a reference to the inserted query job
	 * @throws IOException
	 */
	public String startBigDataQuery(String querySql) throws IOException;

	/**
	 * Polls a big data job and once done save the results to a file
	 * @param jobId
	 * @param filename
	 * @throws IOException
	 */
	public BigInteger saveBigQueryResultsToFile(String jobId, String filename) throws IOException;

	/**
	 * Stream local files into big query
	 * @param files
	 * @param table
	 * @throws IOException
	 */
	public void streamLocalFilesIntoBigData(List<String> files, String table) throws IOException;

	/**
	 * Stream triple data into big query
	 * @param triples
	 * @param table
	 * @throws IOException
	 */
	public void streamTriplesIntoBigData(Collection<Triple> triples, String table) throws IOException;

	

	

}
