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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public abstract class CommonTask implements Task {
	
	protected final static Log log = LogFactory.getLog(CommonTask.class);
	
	protected VMMetaData metadata;
	
	protected CloudService cloud;
	
	protected Results results;
	
	protected Input input;
	

	/**
	 * @param metadata
	 */
	public CommonTask(VMMetaData metadata, CloudService cloud) {
		super();
		this.metadata = metadata;
		this.cloud = cloud;
	}
	
	@Override
	public Results getResults() {
		return this.results;
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.Task#setInput(java.lang.Object)
	 */
	@Override
	public void setInput(Input input) {
		this.input = input;
	}

	
}
