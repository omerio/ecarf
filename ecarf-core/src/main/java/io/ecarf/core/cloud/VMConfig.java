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

	private String zoneId;

	private String imageId;
	
	private String vmType;

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
	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
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
	public void setZoneId(String zoneId) {
		this.zoneId = zoneId;
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
	public void setImageId(String imageId) {
		this.imageId = imageId;
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
	public void setMetaData(VMMetaData metaData) {
		this.metaData = metaData;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
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
	public void setVmType(String vmType) {
		this.vmType = vmType;
	}



}
