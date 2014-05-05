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
package io.ecarf.ccvm.entities;

import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Lists;
import com.google.gson.stream.JsonReader;

/**
 * Job configuration passed to the CCVM
 * Example:
 * {
    "bucket":"swetodblp",
    "schema":"opus_august2007_closure.nt",
    "table":"ontologies.swetodblp",
    "maxFileSizePerNode":83886080,
    "maxTriplesPerNode":5000000,
    "newNodePercentage":0.1,
    "vmType":"n1-standard-2",
    "platform":"google",
    "skipLoad":true,
    "evmAnalysisFiles":[
        "ecarf-evm-1398457340229.json",
        "ecarf-evm-1398457340230.json"
    ]
}
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Job implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 194508794413785108L;

	private String bucket;
	
	private String schema;
	
	private String table;
	
	private Long maxFileSizePerNode;
	
	private Long maxTriplesPerNode;
	
	private Double newNodePercentage;
	
	private String vmType;
	
	private String platform;
	
	private boolean skipLoad;
	
	private List<String> evmAnalysisFiles;

	/**
	 * @return the bucket
	 */
	public String getBucket() {
		return bucket;
	}

	/**
	 * @param bucket the bucket to set
	 */
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	/**
	 * @return the schema
	 */
	public String getSchema() {
		return schema;
	}

	/**
	 * @param schema the schema to set
	 */
	public void setSchema(String schema) {
		this.schema = schema;
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
	public void setTable(String table) {
		this.table = table;
	}

	/**
	 * @return the maxFileSizePerNode
	 */
	public Long getMaxFileSizePerNode() {
		return maxFileSizePerNode;
	}

	/**
	 * @param maxFileSizePerNode the maxFileSizePerNode to set
	 */
	public void setMaxFileSizePerNode(Long maxFileSizePerNode) {
		this.maxFileSizePerNode = maxFileSizePerNode;
	}

	/**
	 * @return the maxTriplesPerNode
	 */
	public Long getMaxTriplesPerNode() {
		return maxTriplesPerNode;
	}

	/**
	 * @param maxTriplesPerNode the maxTriplesPerNode to set
	 */
	public void setMaxTriplesPerNode(Long maxTriplesPerNode) {
		this.maxTriplesPerNode = maxTriplesPerNode;
	}

	/**
	 * @return the newNodePercentage
	 */
	public Double getNewNodePercentage() {
		return newNodePercentage;
	}

	/**
	 * @param newNodePercentage the newNodePercentage to set
	 */
	public void setNewNodePercentage(Double newNodePercentage) {
		this.newNodePercentage = newNodePercentage;
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
	
	/**
	 * @return the platform
	 */
	public String getPlatform() {
		return platform;
	}

	/**
	 * @param platform the platform to set
	 */
	public void setPlatform(String platform) {
		this.platform = platform;
	}

	/**
	 * @return the skipLoad
	 */
	public boolean isSkipLoad() {
		return skipLoad;
	}

	/**
	 * @param skipLoad the skipLoad to set
	 */
	public void setSkipLoad(boolean skipLoad) {
		this.skipLoad = skipLoad;
	}

	/**
	 * @return the evmAnalysisFiles
	 */
	public List<String> getEvmAnalysisFiles() {
		return evmAnalysisFiles;
	}

	/**
	 * @param evmAnalysisFiles the evmAnalysisFiles to set
	 */
	public void setEvmAnalysisFiles(List<String> evmAnalysisFiles) {
		this.evmAnalysisFiles = evmAnalysisFiles;
	}

	/**
	 * Convert to JSON
	 * @return
	 */
	public String toJson() {
		return Utils.GSON.toJson(this);
	}
	
	/**
	 * Read a job instance from a json file
	 * @param jsonFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static Job fromJson(String jsonFile) throws FileNotFoundException, IOException {
		try(FileReader reader = new FileReader(jsonFile)) {
			return Utils.GSON.fromJson(new JsonReader(reader), Job.class);
		}
	}
	
	/**
	 * Create a default job
	 * @return
	 */
	public static Job getDefaultJob() {
		Job job = new Job();
		job.bucket = "swetodblp";
		job.maxFileSizePerNode = FileUtils.ONE_MB * 80;
		job.maxTriplesPerNode = 5_000_000L;
		job.newNodePercentage = 0.1;
		job.platform = Constants.GOOGLE;
		job.schema = "opus_august2007_closure.nt";
		job.table = "ontologies.swetodblp";
		job.vmType = "n1-standard-2";
		job.skipLoad = true;
		job.evmAnalysisFiles = Lists.newArrayList("ecarf-evm-1398457340229.json", "ecarf-evm-1398457340230.json");
		
		return job;
	}
}
