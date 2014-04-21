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

import io.ecarf.core.partition.Item;

import java.util.List;

/**
 * An input to a task
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Input {
	
	/**
	 * A collection of files, terms, etc...
	 */
	private List<String> items;
	
	/**
	 * A list of items that have a key and weight, could be terms/occurrences or files/sizes
	 */
	private List<Item> weightedItems;
	
	private String bucket;
	
	private String table;
	
	// "centos-cloud/global/images/centos-6-v20140318"
	private String imageId;
	
	//"default"
	private String networkId;
	
	//"f1-micro"
	private String vmType;
	
	private String startupScript;
	
	//Config.getLongProperty(Constants.FILE_BIN_CAP_KEY, FileUtils.ONE_GB)
	private Long weightPerNode;
	
	//Config.getDoubleProperty(Constants.TERM_NEW_BIN_KEY, 0.1)
	private Double newBinPercentage;
	
	// should be supplied if we want the evms to do term analysis
	private String schemaTermsFile;
	
	private String schemaFile;
	
	/**
	 * VM names
	 */
	private List<String> nodes;
	

	/**
	 * @return the items
	 */
	public List<String> getItems() {
		return items;
	}

	/**
	 * @param items the items to set
	 */
	public Input setItems(List<String> items) {
		this.items = items;
		return this;
	}

	/**
	 * @return the bucket
	 */
	public String getBucket() {
		return bucket;
	}

	/**
	 * @param bucket the bucket to set
	 */
	public Input setBucket(String bucket) {
		this.bucket = bucket;
		return this;
	}

	/**
	 * @return the imageId
	 */
	public String getImageId() {
		return imageId;
	}

	/**
	 * @param imageId the imageId to set
	 */
	public Input setImageId(String imageId) {
		this.imageId = imageId;
		return this;
	}

	/**
	 * @return the networkId
	 */
	public String getNetworkId() {
		return networkId;
	}

	/**
	 * @param networkId the networkId to set
	 */
	public Input setNetworkId(String networkId) {
		this.networkId = networkId;
		return this;
	}

	/**
	 * @return the vmType
	 */
	public String getVmType() {
		return vmType;
	}

	/**
	 * @param vmType the vmType to set
	 */
	public Input setVmType(String vmType) {
		this.vmType = vmType;
		return this;
	}

	/**
	 * @return the startupScript
	 */
	public String getStartupScript() {
		return startupScript;
	}

	/**
	 * @param startupScript the startupScript to set
	 */
	public Input setStartupScript(String startupScript) {
		this.startupScript = startupScript;
		return this;
	}

	/**
	 * @return the schemaTermsFile
	 */
	public String getSchemaTermsFile() {
		return schemaTermsFile;
	}

	/**
	 * @param schemaTermsFile the schemaTermsFile to set
	 */
	public Input setSchemaTermsFile(String schemaTermsFile) {
		this.schemaTermsFile = schemaTermsFile;
		return this;
	}

	/**
	 * @return the weightPerNode
	 */
	public Long getWeightPerNode() {
		return weightPerNode;
	}

	/**
	 * @param weightPerNode the weightPerNode to set
	 */
	public Input setWeightPerNode(Long weightPerNode) {
		this.weightPerNode = weightPerNode;
		return this;
	}

	/**
	 * @return the newBinPercentage
	 */
	public Double getNewBinPercentage() {
		return newBinPercentage;
	}

	/**
	 * @param newBinPercentage the newBinPercentage to set
	 */
	public Input setNewBinPercentage(Double newBinPercentage) {
		this.newBinPercentage = newBinPercentage;
		return this;
	}

	/**
	 * @return the weightedItems
	 */
	public List<Item> getWeightedItems() {
		return weightedItems;
	}

	/**
	 * @param weightedItems the weightedItems to set
	 */
	public Input setWeightedItems(List<Item> weightedItems) {
		this.weightedItems = weightedItems;
		return this;
	}

	/**
	 * @return the table
	 */
	public String getTable() {
		return table;
	}

	/**
	 * @param table the table to set
	 */
	public Input setTable(String table) {
		this.table = table;
		return this;
	}

	/**
	 * @return the nodes
	 */
	public List<String> getNodes() {
		return nodes;
	}

	/**
	 * @param nodes the nodes to set
	 */
	public Input setNodes(List<String> nodes) {
		this.nodes = nodes;
		return this;
	}

	/**
	 * @return the schemaFile
	 */
	public String getSchemaFile() {
		return schemaFile;
	}

	/**
	 * @param schemaFile the schemaFile to set
	 */
	public Input setSchemaFile(String schemaFile) {
		this.schemaFile = schemaFile;
		return this;
	}

}
