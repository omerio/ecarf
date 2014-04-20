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
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.storage.StorageObject;
import io.ecarf.core.cloud.task.CommonTask;
import io.ecarf.core.cloud.task.Results;
import io.ecarf.core.partition.Item;
import io.ecarf.core.partition.PartitionFunction;
import io.ecarf.core.partition.PartitionFunctionFactory;
import io.ecarf.core.utils.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Read a list of files from cloud storage and based on their size split them
 * using a bin packing algorithm
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class PartitionLoadTask extends CommonTask {
	

	public PartitionLoadTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("Processing partition load");
		
		String bucket = this.input.getBucket();
		
		List<StorageObject> objects = this.cloud.listCloudStorageObjects(bucket);
		
		List<Item> items = new ArrayList<>();
		
		for(StorageObject object: objects) {
			String filename = object.getName();
			if(filename.endsWith(Constants.COMPRESSED_N_TRIPLES)) {
				items.add(new Item(filename, object.getSize().longValue()));
			} else {
				log.warning("Skipping file: " + filename);
			}
		}
		
		// each node should handle a gigbyte of data
		// read it the configurations
		PartitionFunction function = PartitionFunctionFactory.createBinPacking(items, 
				this.input.getNewBinPercentage(), 
				this.input.getWeightPerNode());
		List<List<Item>> bins = function.partition();
		
		this.results = new Results();
		results.setBins(bins);
		
		log.info("Successfully processed partition load");
	}
	

}
