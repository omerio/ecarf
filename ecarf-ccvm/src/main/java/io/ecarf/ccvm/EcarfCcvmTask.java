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
import static io.ecarf.core.utils.Constants.ZONE_KEY;
import io.ecarf.ccvm.entities.Job;
import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMConfig;
import io.ecarf.core.cloud.EcarfMetaData;
import io.ecarf.core.cloud.impl.google.GoogleCloudService;
import io.ecarf.core.cloud.task.Input;
import io.ecarf.core.cloud.task.Results;
import io.ecarf.core.cloud.task.Task;
import io.ecarf.core.cloud.task.TaskFactory;
import io.ecarf.core.cloud.task.coordinator.BigDataLoadTask;
import io.ecarf.core.cloud.task.coordinator.PartitionLoadTask;
import io.ecarf.core.cloud.task.coordinator.SchemaTermCountTask;
import io.ecarf.core.cloud.task.impl.distribute.DistributeLoadTask;
import io.ecarf.core.cloud.task.impl.distribute.DistributeReasonTask;
import io.ecarf.core.cloud.task.impl.distribute.DistributeUploadOutputLogTask;
import io.ecarf.core.cloud.task.impl.partition.PartitionReasonTask;
import io.ecarf.core.cloud.types.TaskType;
import io.ecarf.core.exceptions.NodeException;
import io.ecarf.core.partition.Item;
import io.ecarf.core.partition.Partition;
import io.ecarf.core.utils.Config;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * Run using maven:
 * mvn -q exec:java -Dexec.args="/home/omerio/job.json" > /home/omerio/output.log 2>&1 & exit 0
 * @author Omer Dawelbeit (omerio)
 *
 */
public class EcarfCcvmTask {

	private final static Log log = LogFactory.getLog(EcarfCcvmTask.class);

	private CloudService service;

	private EcarfMetaData metadata;

	private Job job;


	public void run() throws IOException {

		String bucket = this.job.getBucket();
		String schema = this.job.getSchema();
		String table = this.job.getTable();
		Task task;
		Results results;
		Input input;
		List<String> nodes;
		List<String> allNodes = null;
		
		Stopwatch stopwatch = new Stopwatch();
		stopwatch.start();
		
		try {

			if(!this.job.isSkipLoad()) {

				// 1- load the schema and do a count of the relevant terms
				metadata = new EcarfMetaData();
				metadata.addValue(EcarfMetaData.ECARF_BUCKET, bucket);
				metadata.addValue(EcarfMetaData.ECARF_SCHEMA, schema);
				task = new SchemaTermCountTask(metadata, service);
				task.run();

				// 2- partition the instance files into bins
				input = (new Input()).setBucket(bucket).setWeightPerNode(this.job.getMaxFileSizePerNode())
						.setNewBinPercentage(0.0).setNumberOfNodes(this.job.getNumberOfNodes());

				task = new PartitionLoadTask(null, service);
				task.setInput(input);
				task.run();

				results = task.getResults();

				List<String> nodeFiles = results.getBinItems();

				for(String files: nodeFiles) {
					log.info("Partitioned files: " + files + "\n");
				}

				// 3- Distribute the load task between the various nodes
				input = (new Input()).setBucket(bucket).setItems(nodeFiles)
						.setImageId(Config.getProperty(Constants.IMAGE_ID_KEY))
						.setNetworkId(Config.getProperty(Constants.NETWORK_ID_KEY))
						.setStartupScript(Config.getProperty(Constants.STARTUP_SCRIPT_KEY))
						.setSchemaTermsFile(Constants.SCHEMA_TERMS_JSON)
						.setVmType(this.job.getVmType())
						.setDiskType(this.job.getDiskType());

				task = new DistributeLoadTask(null, service);
				task.setInput(input);
				task.run();

				results = task.getResults();

				nodes = results.getNodes(); 
				
				

			} else {
				log.info("Skipping load task");
				results = this.getResults(bucket, this.job.getEvmAnalysisFiles());
				nodes = new ArrayList<>();
			}
			
			log.info("TIMER# Completed files processing in: " + stopwatch);	

			log.info("Active nodes: " + nodes);
			
			allNodes = nodes;

			if(!this.job.isSkipReason()) {

				List<Item> items = results.getItems(); 

				log.info("Term stats for reasoning task split: " + items);

				// 4- get the schema terms stats which was generated by each node
				// for smaller term occurrences such as less than 5million we might want to set a maximum 
				// rather than the largest term occurrences

				input = (new Input()).setWeightedItems(items)
						.setNewBinPercentage(this.job.getNewNodePercentage())
						// 5 million triples per node for swetodblp
						.setWeightPerNode(this.job.getMaxTriplesPerNode())
						.setNumberOfNodes(this.job.getNumberOfNodes());
				
				task = new PartitionReasonTask(null, service);
				task.setInput(input);
				task.run(); 

				results = task.getResults();

				List<Partition> bins = results.getBins();

				log.info("Total number of required evms is: " + bins.size());

				for(Partition bin: bins) {
					log.info("Set: " + bin + ", Sum: " + bin.sum() + "\n");
					for(Item item: bin.getItems()) {
						System.out.println(item.getKey() + "," + item.getWeight());
					}
				}

				List<String> terms = results.getBinItems();

				// 5- Load the generated files into Big Data table
				input = (new Input()).setBucket(bucket).setTable(table);	
				task = new BigDataLoadTask(null, service);
				task.setInput(input);
				task.run();
				
				log.info("TIMER# Completed loading phase in: " + stopwatch);
				stopwatch.reset();
				stopwatch.start();

				// 6- distribute the reasoning between the nodes
				input = (new Input()).setItems(terms)
						.setBucket(bucket).setTable(table)
						.setSchemaFile(schema)
						.setNodes(nodes)
						.setImageId(Config.getProperty(Constants.IMAGE_ID_KEY))
						.setNetworkId(Config.getProperty(Constants.NETWORK_ID_KEY))
						.setStartupScript(Config.getProperty(Constants.STARTUP_SCRIPT_KEY))
						.setZoneId(Config.getProperty(ZONE_KEY))
						.setVmType(this.job.getVmType())
						.setDiskType(this.job.getDiskType())
						// do we stream the inferred data or not
						.setDataLoad(this.job.getDataLoad());

				task = new DistributeReasonTask(null, service);
				task.setInput(input);
				task.run();

				// All nodes, which we can shut down
				allNodes = task.getResults().getNodes();
				log.info("All active nodes: " + allNodes);

			} else {
				allNodes = nodes;
			}
			
			stopwatch.stop();
			log.info("TIMER# Completed reasoning phase in: " + stopwatch);

		} catch(NodeException ne) {
			log.error("Some processing nodes have failed", ne);
			
		} catch(IOException ie) {
			log.error("An error has occurred", ie);
		}
		
		

		// upload logs and shutdown
		if((allNodes != null) && !allNodes.isEmpty()) {
			// 7- all nodes to upload their logs
			input = (new Input()).setBucket(bucket).setNodes(allNodes)
					.setZoneId(Config.getProperty(ZONE_KEY));

			task = new DistributeUploadOutputLogTask(null, service);
			task.setInput(input);
			task.run();

			if(this.job.isShutdown()) {
				// shutdown active nodes
				List<VMConfig> configs = new ArrayList<>();
				for(String nodeId: allNodes) {
					configs.add(new VMConfig().setInstanceId(nodeId));
				}
				this.service.shutdownInstance(configs);
			}
		}
		
		// upload the logs
		log.info("Uploading coordinator output log");
		EcarfMetaData data = new EcarfMetaData();
		data.addValue(EcarfMetaData.ECARF_BUCKET, bucket)
			.addValue(EcarfMetaData.ECARF_TASK, TaskType.UPLOAD_LOGS.toString());
		task = TaskFactory.getTask(data, this.service);
		task.run();
		

	}

	/**
	 * TODO remove
	 * @param bucket
	 * @return
	 * @throws IOException 
	 */
	private Results getResults(String bucket, List<String> nodeFiles) throws IOException {
		Results results = new Results();
		Map<String, Long> allTermStats = new HashMap<String, Long>();

		Set<String> files = new HashSet<>(nodeFiles);

		for(String file: files) {	

			String localStatsFile = Utils.TEMP_FOLDER + file;
			this.service.downloadObjectFromCloudStorage(file, localStatsFile, bucket);

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

		}

		if(!allTermStats.isEmpty()) {

			List<Item> items = new ArrayList<>();
			for(Entry<String, Long> item: allTermStats.entrySet()) {
				Item anItem = (new Item()).setKey(item.getKey()).setWeight(item.getValue());
				items.add(anItem);
			}

			results.setItems(items);

			log.info("Successfully created term stats: " + items);

		}

		return results;
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
	public void setMetadata(EcarfMetaData metadata) {
		this.metadata = metadata;
	}

	/**
	 * @param job the job to set
	 */
	public void setJob(Job job) {
		this.job = job;
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		//System.out.println(Job.getDefaultJob().toJson());

		if(args.length != 1) {
			System.out.println("usage EcarfEvmTask <jobConfigJsonFile>");
			System.exit(1);
		}

		EcarfCcvmTask task = new EcarfCcvmTask();
		EcarfMetaData metadata = null;
		Job job = Job.fromJson(args[0]);
		String platform = job.getPlatform();

		switch(platform) {
		case GOOGLE:
			GoogleCloudService service = new GoogleCloudService();
			try {
				metadata = service.inti();
				//TestUtils.prepare(service);
				task.setService(service);
				task.setJob(job);
				task.setMetadata(metadata);

			} catch(IOException e) {
				log.error("Failed to start cvm program", e);
				throw e;
			}
			break;

		case AMAZON:
			throw new NotImplementedException("Platform not implemented: " + platform);


		default:
			throw new IllegalArgumentException("Unknow platform: " + platform);
		}
		try {
			task.run();
			System.exit(0);

		} catch(Exception e) {
			log.error("Failed to run CCVM", e);
			System.exit(1);
		}
	}

}
