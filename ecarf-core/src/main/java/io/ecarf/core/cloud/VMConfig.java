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
package io.ecarf.core.cloud;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class VMConfig {

	private String instanceId;

	// https://www.googleapis.com/compute/v1/projects/ecarf-1000/zones/us-central1-a
	private String zoneId;

	// for Google, this is an example
	// https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20140318
	private String imageId;

	// https://www.googleapis.com/compute/v1/projects/ecarf-1000/zones/us-central1-a/machineTypes/f1-micro
	private String vmType;

	// for Google, this is an example
	// https://www.googleapis.com/compute/v1/projects/ecarf-1000/global/networks/default
	private String networkId;
	
	private String startupScript;

	private VMMetaData metaData;

	/**
	 * @return the instanceId
	 */
	public String getInstanceId() {
		return instanceId;
	}

	/**
	 * @param instanceId the instanceId to set
	 */
	public VMConfig setInstanceId(String instanceId) {
		this.instanceId = instanceId;
		return this;
	}

	/**
	 * @return the zoneId
	 */
	public String getZoneId() {
		return zoneId;
	}

	/**
	 * @param zoneId the zoneId to set
	 */
	public VMConfig setZoneId(String zoneId) {
		this.zoneId = zoneId;
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
	public VMConfig setImageId(String imageId) {
		this.imageId = imageId;
		return this;
	}

	/**
	 * @return the metaData
	 */
	public VMMetaData getMetaData() {
		return metaData;
	}

	/**
	 * @param metaData the metaData to set
	 */
	public VMConfig setMetaData(VMMetaData metaData) {
		this.metaData = metaData;
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
	public VMConfig setVmType(String vmType) {
		this.vmType = vmType;
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
	public VMConfig setNetworkId(String networkId) {
		this.networkId = networkId;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
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
	public void setStartupScript(String startupScript) {
		this.startupScript = startupScript;
	}

}
