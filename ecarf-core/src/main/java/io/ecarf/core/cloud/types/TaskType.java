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
package io.ecarf.core.cloud.types;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public enum TaskType {
	
	LOAD, 
	REASON_DIRECT, // direct upload into big data
	REASON_HYBRID_DIRECT, // direct upload into big data
	REASON_STREAM, // stream into big data
	REASON_HYBRID_CLOUD_STORAGE, // upload via cloud storage
	REASON_SINGLE_QUERY,
	COUNT, 
	UPLOAD_LOGS;
	
	/**
	 * Get the task type for the particular
	 * @param loadType
	 * @return
	 */
	public static TaskType getReasonTaskTypeForDataLoadType(DataLoadType loadType) {
		TaskType task;
		switch(loadType) {
		case CLOUD_STORAGE:
			task = TaskType.REASON_HYBRID_CLOUD_STORAGE;
			break;

		case STREAM:
			task = TaskType.REASON_STREAM;
			break;
			
		case HYBRID:
			task = TaskType.REASON_HYBRID_DIRECT;
			break;
			
		case CLOUD_STORAGE_SQ:
			task = TaskType.REASON_SINGLE_QUERY;

		case DIRECT:
		default:
			task = TaskType.REASON_DIRECT;
			break;

		}
		return task;
	}

}
