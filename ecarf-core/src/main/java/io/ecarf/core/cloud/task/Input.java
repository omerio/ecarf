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
	
	private String bucket;
	
	// "centos-cloud/global/images/centos-6-v20140318"
	private String imageId;
	
	//"default"
	private String networkId;
	
	//"f1-micro"
	private String vmType;

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

}
