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
package io.ecarf.core.cloud.task.impl.reason;

import io.ecarf.core.cloud.CloudService;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class SaveResultsSubTask implements Callable<Void> {
	
	private final static Log log = LogFactory.getLog(SaveResultsSubTask.class);
	
	private Term term;
	private CloudService cloud;

	/**
	 * 
	 */
	public SaveResultsSubTask(Term term, CloudService cloud) {
		this.term = term;
		this.cloud = cloud;
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Callable#call()
	 */
	@Override
	public Void call() throws Exception {
		
		BigInteger rows = BigInteger.ZERO;
		
		try {
			// block and wait for each job to complete then save results to a file
			
			rows = this.cloud.saveBigQueryResultsToFile(term.getJobId(), term.getFilename());

		} catch(IOException ioe) {
			// transient backend errors
			log.warn("failed to save query results to file, jobId: " + term.getJobId(), ioe);
		}

		log.info("Query found " + rows + ", rows");
		
		term.setRows(rows);
		
		return null;
	}

}
