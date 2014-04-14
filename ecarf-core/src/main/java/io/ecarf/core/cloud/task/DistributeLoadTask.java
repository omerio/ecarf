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
import io.ecarf.core.cloud.storage.StorageObject;
import io.ecarf.core.partition.Item;
import io.ecarf.core.partition.PartitionFunction;
import io.ecarf.core.partition.PartitionFunctionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Read a list of files from cloud storage and based on their size split them
 * using a bin packing algorithm
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DistributeLoadTask extends CommonTask {
	
	private List<String> filesPerNode;

	public DistributeLoadTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("Processing partition load");
		
		String bucket = metadata.getBucket();
		
		List<StorageObject> objects = this.cloud.listCloudStorageObjects(bucket);
		
		List<Item> items = new ArrayList<>();
		
		for(StorageObject object: objects) {
			items.add(new Item(object.getName(), object.getSize().longValue()));
		}
		
		// each node should handle a gigbyte of data
		PartitionFunction function = PartitionFunctionFactory.createBinPacking(items, 0.0, FileUtils.ONE_GB);
		List<List<Item>> bins = function.partition();
		
		if(bins.size() > 0) {
			this.filesPerNode = new ArrayList<>();
			
			for(List<Item> bin: bins) {
				//System.out.println("Set total: " + bin.size() + ", Set" + bin + ", Sum: " + Utils.sum(bin) + "\n");
				List<String> files = new ArrayList<>();
				for(Item item: bin) {
					files.add(item.getKey());
				}
				
				this.filesPerNode.add(StringUtils.join(files, ','));
			}
		}
		
		for(String files: this.filesPerNode) {
			log.info("Partitioned files: " + files + "\n");
		}
		
		log.info("Successfully processed partition load");
	}
	
	@Override
	public Object getResults() {
		
		return filesPerNode;
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#setInput(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void setInput(Object input) {
		this.filesPerNode = (List<String>) input;
	}
	
	
	public class Results {
		private List<String> nodes;
		
		private List<Item> items;
	}

}
