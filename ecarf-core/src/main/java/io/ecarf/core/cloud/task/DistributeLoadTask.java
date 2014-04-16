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
import io.ecarf.core.cloud.VMConfig;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.types.TaskType;
import io.ecarf.core.cloud.types.VMStatus;
import io.ecarf.core.partition.Item;
import io.ecarf.core.partition.PartitionFunction;
import io.ecarf.core.partition.PartitionFunctionFactory;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
				
				String instanceId = VMMetaData.ECARF_VM_PREFIX + (timestamp + count);
				VMConfig conf = new VMConfig();
				conf.setImageId(input.getImageId())
					.setInstanceId(instanceId)
					.setMetaData(metaData)
					.setNetworkId(input.getNetworkId())
					.setVmType(input.getVmType())
					.setStartupScript(input.getStartupScript());
				
				this.results.getNodes().add(instanceId);

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
						
						VMMetaData metaData = this.cloud.getEcarfMetaData(instanceId, null);
						ready = VMStatus.READY.equals(metaData.getVMStatus());
					
					} while (!ready);
				}
				
				// all done, now get the results from cloud storage and combine the schema terms stats
				Map<String, Long> allTermStats = new HashMap<String, Long>();
				
				for(String instanceId: this.results.getNodes()) {	
					String statsFile = instanceId + Constants.DOT_JSON;
					
					String localStatsFile = Utils.TEMP_FOLDER + statsFile;
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
					
				}
				
				if(!allTermStats.isEmpty()) {
					
					List<Item> items = new ArrayList<>();
					for(Entry<String, Long> item: allTermStats.entrySet()) {
						Item anItem = (new Item()).setKey(item.getKey()).setWeight(item.getValue());
						items.add(anItem);
					}
					
					// each node can handle up to 10% more than the largest term
					// read from the configurations
					PartitionFunction function = PartitionFunctionFactory.createBinPacking(items, 
							this.input.getNewBinTermPercent(), null);
					List<List<Item>> bins = function.partition();
					
					this.results = new Results();
					results.setBins(bins);
					
					log.info("Successfully created term stats: " + bins);
					
				}

			} else {
				// TODO retry and error handling
				throw new IOException("Some eVMs have failed to start");
			}
			

		}

		log.info("Successfully processed distribute load");
	}
	
	
}
