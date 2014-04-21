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
import io.ecarf.core.cloud.task.impl.ProcessLoadTask;
import io.ecarf.core.cloud.task.impl.DoReasonTask;
import io.ecarf.core.cloud.task.impl.SchemaTermCountTask;
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
				task = new ProcessLoadTask(metadata, cloud);
				break;

			case REASON:
				task = new DoReasonTask(metadata, cloud);
				break;

			case COUNT:
				task = new SchemaTermCountTask(metadata, cloud);

			default:
				throw new IllegalArgumentException("Unknown task type: " + type);
			}
		}
		
		return task;
	}
}
