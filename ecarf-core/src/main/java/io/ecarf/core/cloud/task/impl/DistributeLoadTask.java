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

import static io.ecarf.core.cloud.impl.google.GoogleMetaData.NOT_FOUND;
import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMConfig;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.task.CommonTask;
import io.ecarf.core.cloud.task.Results;
import io.ecarf.core.cloud.types.TaskType;
import io.ecarf.core.cloud.types.VMStatus;
import io.ecarf.core.partition.Item;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

import org.apache.commons.lang3.StringUtils;

/**
 * Read a list of files from cloud storage and based on their size split them
 * using a bin packing algorithm
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DistributeLoadTask extends CommonTask {
	

	public DistributeLoadTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {

		log.info("Processing distribute load");

		List<String> nodeFiles = this.input.getItems();

		if((nodeFiles != null) && !nodeFiles.isEmpty()) {
			
			this.results = (new Results()).newNodes();

			String bucket = this.input.getBucket();

			List<VMConfig> vms = new ArrayList<>();

			long timestamp = (new Date()).getTime();
			int count = 0;
			
			for(String files: nodeFiles) {
				VMMetaData metaData = new VMMetaData();
				metaData.addValue(VMMetaData.ECARF_TASK, TaskType.LOAD.toString())
					.addValue(VMMetaData.ECARF_FILES, files)
					.addValue(VMMetaData.ECARF_BUCKET, bucket);
				// do we have a schema terms file
				if(StringUtils.isNotBlank(this.input.getSchemaTermsFile())) {
					metaData.addValue(VMMetaData.ECARF_SCHEMA_TERMS, this.input.getSchemaTermsFile());
				}
				
				String instanceId = VMMetaData.ECARF_VM_PREFIX + (timestamp + count);
				VMConfig conf = new VMConfig();
				conf.setImageId(input.getImageId())
					.setInstanceId(instanceId)
					.setMetaData(metaData)
					.setNetworkId(input.getNetworkId())
					.setVmType(input.getVmType())
					.setDiskType(input.getDiskType())
					.setStartupScript(input.getStartupScript());
				
				this.results.getNodes().add(instanceId);
				vms.add(conf);

				log.info("Create VM Config for " + instanceId);
				count++;
			}
			
			boolean success = this.cloud.startInstance(vms, true);

			if(success) {
				// wait for the VMs to finish their loading
				for(String instanceId: this.results.getNodes()) {	
					boolean ready = false;
					
					do {
						Utils.block(Utils.getApiRecheckDelay());
						// TODO cater for this error
						/*
						 * SEVERE: Failed to run CCVM
							com.google.api.client.googleapis.json.GoogleJsonResponseException: 503 Service Unavailable
							{
							  "code" : 503,
							  "errors" : [ {
							    "domain" : "global",
							    "message" : "Backend Error",
							    "reason" : "backendError"
							  } ],
							  "message" : "Backend Error"
							}

						 */
						VMMetaData metaData = this.cloud.getEcarfMetaData(instanceId, null);
						ready = VMStatus.READY.equals(metaData.getVMStatus());
						// TODO status can be error ERROR
						if(VMStatus.ERROR.equals(metaData.getVMStatus())) {
							// for now we are throwing an exception, in the future need to return a status so tasks can be retried
							throw Utils.exceptionFromEcarfError(metaData, instanceId);
							
						}
						
					} while (!ready);
				}
				
				// all done, now get the results from cloud storage and combine the schema terms stats
				if(StringUtils.isNotBlank(this.input.getSchemaTermsFile())) {
					
					Map<String, Long> allTermStats = new HashMap<String, Long>();
					
					for(String instanceId: this.results.getNodes()) {	
						String statsFile = instanceId + Constants.DOT_JSON;

						String localStatsFile = Utils.TEMP_FOLDER + statsFile;
						
						try {
							this.cloud.downloadObjectFromCloudStorage(statsFile, localStatsFile, bucket);

							// convert from JSON
							Map<String, Long> termStats = Utils.jsonFileToMap(localStatsFile);

							for(Entry<String, Long> term: termStats.entrySet()) {
								String key = term.getKey();
								Long value = term.getValue();

								if(allTermStats.containsKey(key)) {
									value = allTermStats.get(key) + value;
								} 

								allTermStats.put(key, value);
							}

							log.info("Evms analysed: " + allTermStats.size() + ", terms");
						} catch(IOException e) {
							// a file not found means the evm didn't find any schema terms so didn't generate any stats
							log.log(Level.SEVERE, "failed to download file: " + localStatsFile, e);
							if(!(e.getMessage().indexOf(NOT_FOUND) >= 0)) {
								throw e;
							}
						}

					}

					if(!allTermStats.isEmpty()) {

						List<Item> items = new ArrayList<>();
						for(Entry<String, Long> item: allTermStats.entrySet()) {
							Item anItem = (new Item()).setKey(item.getKey()).setWeight(item.getValue());
							items.add(anItem);
						}

						this.results.setItems(items);

						log.info("Successfully created term stats: " + items);
						
					}
				}

			} else {
				// TODO retry and error handling
				throw new IOException("Some eVMs have failed to start");
			}
			

		}

		log.info("Successfully processed distribute load");
	}
	
	
}
