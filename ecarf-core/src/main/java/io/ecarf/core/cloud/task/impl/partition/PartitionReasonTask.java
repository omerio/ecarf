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
package io.ecarf.core.cloud.task.impl.partition;

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.task.CommonTask;
import io.ecarf.core.cloud.task.Results;
import io.ecarf.core.partition.Item;
import io.ecarf.core.partition.Partition;
import io.ecarf.core.partition.PartitionFunction;
import io.ecarf.core.partition.PartitionFunctionFactory;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Read a list of weighted Triple terms and based on their occurrences split them
 * using a bin packing algorithm
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class PartitionReasonTask extends CommonTask {
	
	private final static Log log = LogFactory.getLog(PartitionReasonTask.class);


	public PartitionReasonTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {

		log.info("Processing partition reason");

		List<Item> items = this.input.getWeightedItems();

		// each node can handle up to 10% more than the largest term
		// read from the configurations
		PartitionFunction function;
		
		if(this.input.getNumberOfNodes() == null) {
			
			function = PartitionFunctionFactory.createBinPacking(items, 
					this.input.getNewBinPercentage(), this.input.getWeightPerNode());
		} else {
			function = PartitionFunctionFactory.createBinPacking(items, this.input.getNumberOfNodes());
		}

		List<Partition> bins = function.partition();

		this.results = new Results();
		results.setBins(bins);

		log.info("Successfully created term stats: " + bins);

	}

}
