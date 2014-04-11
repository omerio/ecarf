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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.time.DateUtils;

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
	
	/**
	 * Wait until two values become equal
	 * @param value1
	 * @param value2
	 */
	public void waitForEquality(Object value1, Object value2) {
		// wait until all files have downloaded successfully
		while(!value1.equals(value2)) {
			try {
				Thread.sleep(DateUtils.MILLIS_PER_SECOND * 20);
			} catch (InterruptedException e1) {
				log.log(Level.WARNING, "wait interrupted", e1);
			}
		}
	}
	
	

}
