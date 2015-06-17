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

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.task.impl.DoReasonTask1;
import io.ecarf.core.cloud.task.impl.DoReasonTask2;
import io.ecarf.core.cloud.task.impl.DoUploadOutputLogTask;
import io.ecarf.core.cloud.task.impl.SchemaTermCountTask;
import io.ecarf.core.cloud.task.impl.load.ProcessLoadTask1;
import io.ecarf.core.cloud.task.impl.reason.DoReasonTask3;
import io.ecarf.core.cloud.task.impl.reason.DoReasonTask5;
import io.ecarf.core.cloud.task.impl.reason.phase2.DoReasonTask6;
import io.ecarf.core.cloud.types.TaskType;

/**
 * Returns a task that a evm should run
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TaskFactory {

	/**
	 * Create a task based on the meta data
	 * @param metadata
	 * @return
	 */
	public static Task getTask(VMMetaData metadata, CloudService cloud) {
		
		TaskType type = metadata.getTaskType();
		Task task = null;
		
		if(type != null) {
			
			switch(type) {
			case LOAD:
				task = new ProcessLoadTask1(metadata, cloud);
				break;
				
			case REASON_STREAM:
				// stream into big data
				task = new DoReasonTask1(metadata, cloud);
				break;
				
			case REASON_DIRECT:
				// direct upload into big data
				task = new DoReasonTask2(metadata, cloud);
				break;
				
			case REASON_HYBRID_DIRECT:
				// direct upload into big data
				task = new DoReasonTask3(metadata, cloud);
				break;
				
			case REASON_HYBRID_CLOUD_STORAGE:
				// load through cloud storage
				task = new DoReasonTask5(metadata, cloud);
				break;
				
			case REASON_SINGLE_QUERY:
				task = new DoReasonTask6(metadata, cloud);

			case COUNT:
				task = new SchemaTermCountTask(metadata, cloud);
				break;
				
			case UPLOAD_LOGS:
				task = new DoUploadOutputLogTask(metadata, cloud);
				break;

			default:
				throw new IllegalArgumentException("Unknown task type: " + type);
			}
		}
		
		return task;
	}
}
