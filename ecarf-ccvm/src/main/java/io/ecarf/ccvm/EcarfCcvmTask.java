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
package io.ecarf.ccvm;

import io.cloudex.framework.components.Coordinator;
import io.cloudex.framework.config.Job;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Run using maven:
 * mvn -q exec:java -Dexec.args="/home/omerio/job.json" > /home/omerio/output.log 2>&1 & exit 0
 * @author Omer Dawelbeit (omerio)
 *
 */
public class EcarfCcvmTask {

	private final static Log log = LogFactory.getLog(EcarfCcvmTask.class);



	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		if(args.length != 1) {
			System.out.println("usage EcarfEvmTask <jobConfigJsonFile>");
			System.exit(1);
		}

		Coordinator coordinator = null;
	
		try {
		    
		    Job job = Job.fromJsonFile(args[0]);
		    log.info("Running job from json file: " + args[0] + ", job id: " + job.getId());
            coordinator = new Coordinator.Builder(job, new EcarfGoogleCloudServiceImpl())
                .setShutdownProcessors(true).build();

        } catch(IOException e) {
            log.error("Failed to start the coordinator program", e);
            throw e;
        }
		
		try {
		    coordinator.run();
			System.exit(0);

		} catch(Exception e) {
			log.error("Failed to run CCVM", e);
			System.exit(1);
		}
	}

}
