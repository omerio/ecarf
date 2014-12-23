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
import io.ecarf.core.cloud.VMConfig;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.task.CommonTask;
import io.ecarf.core.cloud.task.Results;
import io.ecarf.core.cloud.types.TaskType;
import io.ecarf.core.cloud.types.VMStatus;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import org.apache.commons.lang3.StringUtils;

import com.google.api.client.util.Lists;
import com.google.common.collect.Sets;

/**
 * Read a list of files from cloud storage and based on their size split them
 * using a bin packing algorithm
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DistributeReasonTask extends CommonTask {
	

	public DistributeReasonTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {

		log.info("Processing distribute reason");

		List<String> nodeTerms = this.input.getItems();
		List<String> activeNodes = (this.input.getNodes() != null) ? 
				Lists.newArrayList(this.input.getNodes()) : new ArrayList<String>();
				
		String zoneId = this.input.getZoneId();
		String bucket = this.input.getBucket();
				
		List<String> reasonNodes = new ArrayList<>();

		if((nodeTerms != null) && !nodeTerms.isEmpty()) {

			List<VMConfig> vms = new ArrayList<>();

			long timestamp = (new Date()).getTime();
			int count = 0;
			int index = 0;
			
			for(String terms: nodeTerms) {
				
				String termsFilename = null;
				if(terms.length() > VMMetaData.MAX_METADATA_SIZE) {
					log.info("Metadata too large, using file. Size = " + terms.length());
					// use files instead
					termsFilename = Constants.NODE_TERMS + index + Constants.DOT_JSON;	
					String termsFile = Utils.TEMP_FOLDER + termsFilename;
					// save to file
					Utils.objectToJsonFile(termsFile, Utils.csvToSet(terms));
					
					// upload the file to cloud storage
					this.cloud.uploadFileToCloudStorage(termsFile, bucket);
					index++;
				}
				
				// re-program existing vms
				if(activeNodes.size() > 0) {
					
					String instanceId = activeNodes.iterator().next();
					activeNodes.remove(activeNodes.indexOf(instanceId));
					VMMetaData metaData = this.cloud.getEcarfMetaData(instanceId, zoneId);
					String fingerprint = metaData.getFingerprint();
					metaData = this.getReasonMetadata(terms, termsFilename);
					metaData.setFingerprint(fingerprint);
					this.cloud.updateInstanceMetadata(metaData, zoneId, instanceId, true);
					reasonNodes.add(instanceId);
					
				 
				} else {
					// start new VMs
					VMMetaData metaData = this.getReasonMetadata(terms, termsFilename);

					String instanceId = VMMetaData.ECARF_VM_PREFIX + (timestamp + count);
					VMConfig conf = new VMConfig();
					conf.setImageId(input.getImageId())
						.setInstanceId(instanceId)
						.setMetaData(metaData)
						.setNetworkId(input.getNetworkId())
						.setVmType(input.getVmType())
						.setDiskType(input.getDiskType())
						.setZoneId(zoneId)
						.setStartupScript(input.getStartupScript());

					reasonNodes.add(instanceId);
		
					vms.add(conf);
					
					log.info("Create VM Config for " + instanceId);
					count++;
				}
			}

			if(!vms.isEmpty()) {
				boolean success = this.cloud.startInstance(vms, true);
				if(!success) {
					throw new IOException("Some eVMs have failed to start");
					// TODO better error handling and retry
				}
			}

			IOException nodeException = null;
			
			// wait for the VMs to finish their loading
			for(String instanceId: reasonNodes) {	
				boolean ready = false;

				do {
					Utils.block(Utils.getApiRecheckDelay());

					VMMetaData metaData = this.cloud.getEcarfMetaData(instanceId, zoneId);
					ready = VMStatus.READY.equals(metaData.getVMStatus());
					// check for ERROR status
					if(VMStatus.ERROR.equals(metaData.getVMStatus())) {
						nodeException = Utils.exceptionFromEcarfError(metaData, instanceId);
						log.log(Level.SEVERE, instanceId + " processing node has failed", nodeException);
					}

				} while (!ready);
			}
			
			// if any of the nodes has failed then throw an exception
			if(nodeException != null) {
				// for now we are throwing an exception, in the future need to return a status so tasks can be retried
				throw nodeException;
			}

			// all done, now shutdown the instances
			this.results = (new Results()).newNodes();
			Set<String> allNodes = Sets.newHashSet(this.input.getNodes());
			allNodes.addAll(reasonNodes);
			this.results.getNodes().addAll(Lists.newArrayList(allNodes));
			
			log.info("Active instances: " + this.results.getNodes());

		}

		log.info("Successfully processed distribute load");
	}
	
	/**
	 * Prepare the vm metadata for reasoning
	 * @param terms
	 * @return
	 */
	private VMMetaData getReasonMetadata(String terms, String termsFile) {
		VMMetaData metaData = new VMMetaData();
		
		if(termsFile != null) {
			metaData.addValue(VMMetaData.ECARF_TERMS_FILE, termsFile);
			
		} else {
			metaData.addValue(VMMetaData.ECARF_TERMS, terms);
		}
		
		TaskType task = null;
		
		if(this.input.isStreamData()) {
			task = TaskType.REASON1;
			log.info("*************************************** Streaming inferred triples into big data service ***************************************");
			
		} else {
			task = TaskType.REASON;
			log.info("*************************************** Asynchronously loading inferred triples into big data service ***************************************");
		}
		
		metaData.addValue(VMMetaData.ECARF_TASK, task.toString())
			.addValue(VMMetaData.ECARF_BUCKET, this.input.getBucket())
			.addValue(VMMetaData.ECARF_TABLE, this.input.getTable());
		// do we have a schema terms file
		if(StringUtils.isNotBlank(this.input.getSchemaFile())) {
			metaData.addValue(VMMetaData.ECARF_SCHEMA, this.input.getSchemaFile());
		}
		return metaData;
	}

	
}
