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
package io.ecarf.evm;

import static io.ecarf.core.utils.Constants.AMAZON;
import static io.ecarf.core.utils.Constants.GOOGLE;
import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.impl.google.GoogleCloudService;
import io.ecarf.core.cloud.task.Task;
import io.ecarf.core.cloud.task.TaskFactory;
import io.ecarf.core.cloud.types.VMStatus;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The program p of the Ecarf framework
 * 
 * @author omerio
 *
 */
public class EcarfEvmTask {
	
	private final static Logger log = Logger.getLogger(EcarfEvmTask.class.getName()); 
	
	/**
	 * Sleep for 10 seconds
	 */
	//private static final long TIMER_DELAY = 10 * DateUtils.MILLIS_PER_SECOND;
	
	private CloudService service;
	
	private VMMetaData metadata;
	
	
	/**
	 * Continuously run until we are shutdown by the ccvm
	 * @param metadata - the initial meta data
	 * @throws IOException
	 */
	public void run() throws IOException {
		
		//final Thread currentThread = Thread.currentThread();
		
		while(true) {
			try {
				// Load task
				
				// set status to BUSY
				metadata.addValue(VMMetaData.ECARF_STATUS, VMStatus.BUSY.toString());
				this.service.updateInstanceMetadata(metadata);

				// run the task
				Task task = TaskFactory.getTask(metadata, service);
				if(task != null) {
					task.run();
				} 
				// else {
				// no task is set, just set status to ready and wait for tasks
				//}
				
				// finished processing
				// blank the task type and set the status to READY
				metadata.clearValues();
				metadata.addValue(VMMetaData.ECARF_STATUS, VMStatus.READY.toString());

				this.service.updateInstanceMetadata(metadata);

				// now wait for any change in the metadata
				log.info("Waiting for new instructions from ccvm");
				metadata = this.service.getEcarfMetaData(true);
				
			} catch(Exception e) {
				
				log.log(Level.SEVERE, "An error has occurred whilst running/waiting for tasks", e);
				// try to update the Metadata to a fail status
				try {
					
					metadata = this.service.getEcarfMetaData(false);
					Utils.exceptionToEcarfError(metadata, e);
					this.service.updateInstanceMetadata(metadata);
					
					// wait until we get further instructions
					// now wait for any change in the metadata
					log.info("Waiting for new instructions from ccvm");
					metadata = this.service.getEcarfMetaData(true);
					
				} catch(Exception e1) {
					// all has failed with no hope of recovery
					log.log(Level.SEVERE, "An error has occurred whilst trying to recover", e);
					// self terminate :-(
					this.service.shutdownInstance();
				}
			}
		}

	}
	
	/**
	 * Wait until we have new instructions to do something else
	 * @param thread
	 */
	/*private void wait(final Thread thread) {
		// create timer, then sleep
		final Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				log.info("--------- Metadata check Timer ---------");

				try {
					VMMetaData md = service.getEcarfMetaData(false);
					if(md.getTaskType() != null) {
						metadata = md;
						timer.cancel();
						// wake if asleep
						LockSupport.unpark(thread);
					}
				} catch (IOException e) {
					log.log(Level.SEVERE, "Failed to retrieve the metadata from the server", e);
				}
			}
		}, TIMER_DELAY, TIMER_DELAY);

		// wait for further instructions from ccvm
		LockSupport.park();
	}*/
	
	/**
	 * @param service the service to set
	 */
	public void setService(CloudService service) {
		this.service = service;
		
	}
	
	/**
	 * @param metadata the metadata to set
	 */
	public void setMetadata(VMMetaData metadata) {
		this.metadata = metadata;
	}

	/**
	 * 1- Read metadata
	 * 2- Compute with metadata
	 * 3- Update Status metadata
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String [] args) throws IOException {
		String platform = GOOGLE;
		if(args.length > 0) {
			platform = args[0];
		}
		
		EcarfEvmTask task = new EcarfEvmTask();
		VMMetaData metadata = null;
		
		switch(platform) {
		case GOOGLE:
			CloudService service = new GoogleCloudService();
			try {
				metadata = service.inti();
				task.setService(service);
				task.setMetadata(metadata);
				
			} catch(IOException e) {
				log.log(Level.SEVERE, "Failed to start evm program", e);
				throw e;
			}
			break;

		case AMAZON:
			break; 

		default:
			System.out.println("usage EcarfEvmTask <platform>");
			System.exit(1);
		}

		task.run();
	}

	

}
