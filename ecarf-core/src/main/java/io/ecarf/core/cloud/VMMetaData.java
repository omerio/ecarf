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

import io.ecarf.core.cloud.types.TaskType;
import io.ecarf.core.cloud.types.VMStatus;
import io.ecarf.core.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A collection of vm instance metadata used by ecarf. 
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class VMMetaData {
	
	private final static Logger log = Logger.getLogger(VMMetaData.class.getName()); 
	
	// the prefix of the vm name
	public static final String ECARF_VM_PREFIX = "ecarf-evm-";
	
	// the task for the evm, currently load and reason
	public static final String ECARF_TASK = "ecarf-task";
	
	// the status of the evm, ready, busy or error
	public static final String ECARF_STATUS = "ecarf-status";
	
	// a list of cloud storage or http files that each evm should process
	public static final String ECARF_FILES = "ecarf-files";
	
	// A storage file that contains the closure of the schema triples
	public static final String ECARF_SCHEMA = "ecarf-schema";
	
	// A storage file that contains all the schema terms
	public static final String ECARF_SCHEMA_TERMS = "ecarf-schema-terms";
	
	// the cloud big data table that should be used for reasoning
	public static final String ECARF_TABLE = "ecarf-table";
	
	// a list of terms that the evm should be responsible for
	public static final String ECARF_TERMS = "ecarf-terms";
	
	// the cloud storage bucket to use for processing the relevant ontology files
	public static final String ECARF_BUCKET = "ecarf-bucket";
	
	// ecarf-exception	java.io.FileNotFoundException
	public static final String ECARF_EXCEPTION = "ecarf-exception";
	
	// ecarf-message	/tmpswetodblp_2008_2.nt.gz (Permission denied)
	public static final String ECARF_MESSAGE = "ecarf-message";
	
	//public static final String ECARF_JOD_ID = "ecarf-job-id";
	
	private Map<String, Object> attributes;
	
	private String fingerprint;

	/**
	 * 
	 */
	public VMMetaData() {
		super();
		this.attributes = new HashMap<>();
	}

	/**
	 * @param attributes
	 */
	public VMMetaData(Map<String, Object> attributes) {
		super();
		this.attributes = attributes;
	}
	
	/**
	 * @param attributes
	 */
	public VMMetaData(Map<String, Object> attributes, String fingerprint) {
		super();
		this.attributes = attributes;
		this.fingerprint = fingerprint;
	}
	
	/**
	 * Get the task type 
	 * @return
	 */
	public TaskType getTaskType() {
		TaskType task = null;
		if(this.attributes.get(ECARF_TASK) != null) {
			try {
				task = TaskType.valueOf((String) this.attributes.get(ECARF_TASK));
				
			} catch(IllegalArgumentException e) {
				log.log(Level.SEVERE, "Failed to parse task type", e);
			}
		}
		return task;
	}
	
	/**
	 * Get the vm status
	 * @return
	 */
	public VMStatus getVMStatus() {
		VMStatus vmStatus = null;
		if(this.attributes.get(ECARF_STATUS) != null) {
			try {
				vmStatus = VMStatus.valueOf((String) this.attributes.get(ECARF_STATUS));
				
			} catch(IllegalArgumentException e) {
				log.log(Level.SEVERE, "Failed to parse vm status", e);
			}
		}
		
		return vmStatus;
	}
	
	/**
	 * Get the bucket
	 * @return
	 */
	public String getBucket() {
		return (String) this.attributes.get(ECARF_BUCKET);
	}
	
	/**
	 * Get the schema terms file
	 * @return
	 */
	public String getSchemaTermsFile() {
		return (String) this.attributes.get(ECARF_SCHEMA_TERMS);
	}
	
	/**
	 * Return a set of files
	 * @return
	 */
	public Set<String> getFiles() {
		String filesStr = (String) this.attributes.get(ECARF_FILES);
		return Utils.csvToSet(filesStr);
	}
	
	/**
	 * Return a set of terms
	 * @return
	 */
	public Set<String> getTerms() {
		String termsStr = (String) this.attributes.get(ECARF_TERMS);
		return Utils.csvToSet(termsStr);
	}
	
	
	
	/**
	 * Get the exception if any
	 * @return
	 */
	public String getException() {
		return (String) this.attributes.get(ECARF_EXCEPTION);
	}
	
	/**
	 * Get the message if any
	 * @return
	 */
	public String getMessage() {
		return (String) this.attributes.get(ECARF_MESSAGE);
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public String getValue(String key) {
		return (String) this.attributes.get(key);
	}
	
	/**
	 * add a value to the metadata, return this instance for chaining
	 * @param key
	 * @param value
	 */
	public VMMetaData addValue(String key, String value) {
		this.attributes.put(key, value);
		return this;
	}

	/**
	 * 
	 * @see java.util.Map#clear()
	 */
	public void clearValues() {
		attributes.clear();
	}

	/**
	 * @return the attributes
	 */
	public Map<String, Object> getAttributes() {
		return attributes;
	}

	/**
	 * @return the fingerprint
	 */
	public String getFingerprint() {
		return fingerprint;
	}

	/**
	 * @param fingerprint the fingerprint to set
	 */
	public void setFingerprint(String fingerprint) {
		this.fingerprint = fingerprint;
	}

}
