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
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class CommonTask implements Task {
	
	protected final static Logger log = Logger.getLogger(CommonTask.class.getName()); 
	
	protected VMMetaData metadata;
	
	protected CloudService cloud;
	

	/**
	 * @param metadata
	 */
	public CommonTask(VMMetaData metadata, CloudService cloud) {
		super();
		this.metadata = metadata;
		this.cloud = cloud;
	}


	@Override
	public void run() throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}
	
}
