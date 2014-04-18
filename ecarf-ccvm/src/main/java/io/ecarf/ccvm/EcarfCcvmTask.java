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
package io.ecarf.ccvm;

import static io.ecarf.core.utils.Constants.AMAZON;
import static io.ecarf.core.utils.Constants.GOOGLE;
import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.impl.google.GoogleCloudService;
import io.ecarf.core.cloud.task.DistributeLoadTask;
import io.ecarf.core.cloud.task.Input;
import io.ecarf.core.cloud.task.PartitionLoadTask;
import io.ecarf.core.cloud.task.Results;
import io.ecarf.core.cloud.task.SchemaTermCountTask;
import io.ecarf.core.cloud.task.Task;
import io.ecarf.core.partition.Item;
import io.ecarf.core.utils.Config;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.TestUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class EcarfCcvmTask {
	
	private final static Logger log = Logger.getLogger(EcarfCcvmTask.class.getName()); 
	
	private CloudService service;
	
	private VMMetaData metadata;
	
	
	public void run() throws IOException {
		
		String bucket = "swetodblp";
		String schema = "opus_august2007_closure.nt";
		
		// 1- load the schema and do a count of the relevant terms
		VMMetaData metadata = new VMMetaData();
		metadata.addValue(VMMetaData.ECARF_BUCKET, bucket);
		metadata.addValue(VMMetaData.ECARF_SCHEMA, schema);
		Task task = new SchemaTermCountTask(metadata, service);
		task.run();
		
		// 2- partition the instance files into bins
		Input input = (new Input()).setBucket(bucket).setFileSizePerNode(FileUtils.ONE_MB * 80);
		task = new PartitionLoadTask(null, service);
		task.setInput(input);
		task.run();
		
		Results results = task.getResults();
		
		List<String> nodeFiles = results.getBinItems();
		
		for(String files: nodeFiles) {
			log.info("Partitioned files: " + files + "\n");
		}
		
		// 3- Distribute the load task between the various nodes
		input = (new Input()).setBucket(bucket).setItems(nodeFiles)
				.setImageId(Config.getProperty(Constants.IMAGE_ID_KEY))
				.setNetworkId(Config.getProperty(Constants.NETWORK_ID_KEY))
				.setVmType(Config.getProperty(Constants.VM_TYPE_KEY))
				.setStartupScript(Config.getProperty(Constants.STARTUP_SCRIPT_KEY))
				.setNewBinTermPercent(Config.getDoubleProperty(Constants.TERM_NEW_BIN_KEY, 0.1));
		task = new DistributeLoadTask(null, service);
		task.setInput(input);
		task.run();
		
		results = task.getResults();
		
		List<List<Item>> bins = results.getBins();
		
		for(List<Item> bin: bins) {
			log.info("Set: " + bin + ", Sum: " + Utils.sum(bin) + "\n");
		}
	}
	
	
	/**
	 * @param service the service to set
	 */
	public void setService(CloudService service) {
		this.service = service;
		
	}
	
	/**
	 * @param metadata the metadata to set
	 */
	public void setMetadata(VMMetaData metadata) {
		this.metadata = metadata;
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String platform = GOOGLE;
		if(args.length > 0) {
			platform = args[0];
		}
		
		EcarfCcvmTask task = new EcarfCcvmTask();
		VMMetaData metadata = null;

		switch(platform) {
		case GOOGLE:
			GoogleCloudService service = new GoogleCloudService();
			//try {
			//metadata = service.inti();
			TestUtils.prepare(service);
			task.setService(service);
			task.setMetadata(metadata);

			/*} catch(IOException e) {
				log.log(Level.SEVERE, "Failed to start evm program", e);
				throw e;
			}*/
			break;

		case AMAZON:
			break; 

		default:
			System.out.println("usage EcarfEvmTask <platform>");
			System.exit(1);
		}

		task.run();
	}

}
