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
import io.ecarf.core.cloud.types.VMStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.time.DateUtils;

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
	private static final long TIMER_DELAY = 10 * DateUtils.MILLIS_PER_SECOND;
	
	private CloudService service;
	
	private VMMetaData metadata;
	
	
	/**
	 * Continously run until we are shutdown by the ccvm
	 * @param metadata - the initial meta data
	 * @throws IOException
	 */
	public void run() throws IOException {
		
		final Thread currentThread = Thread.currentThread();
		Map<String, String> items = new HashMap<>();
		
		while(true) {
			
			// Load task
			// read the files from http:// or from gs://
			// download files locally (gziped)
			// read through the files counting the relevant terms and rewriting into bigquery format (comma separated)
			// set status to BUSY
			items.put(VMMetaData.ECARF_STATUS, VMStatus.BUSY.toString());
			this.service.updateInstanceMetadata(items);
			
			switch(metadata.getTaskType()) {
			case LOAD:
				log.info("Current Task LOAD");
				try {
					Thread.sleep(DateUtils.MILLIS_PER_SECOND * 20);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				break;
				
			case REASON:
				log.info("Current Task REASON");
				try {
					Thread.sleep(DateUtils.MILLIS_PER_SECOND * 20);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				break;
			
			}
			// finished processing
			// blank the task type and set the status to READY
			items.put(VMMetaData.ECARF_STATUS, VMStatus.READY.toString());
			items.put(VMMetaData.ECARF_TASK, "");
			this.service.updateInstanceMetadata(items); 
			
			// create timer, then sleep
			final Timer timer = new Timer();
			timer.scheduleAtFixedRate(new TimerTask() {

				@Override
				public void run() {
					log.info("--------- Metadata check Timer ---------");
					
					try {
						VMMetaData md = service.getEcarfMetaData();
						if(md.getTaskType() != null) {
							metadata = md;
							timer.cancel();
							// wake if asleep
							LockSupport.unpark(currentThread);
						}
					} catch (IOException e) {
						log.log(Level.SEVERE, "Failed to retrieve the metadata from the server", e);
					}
				}
			}, TIMER_DELAY, TIMER_DELAY);
			
			// wait for further instructions from ccvm
			LockSupport.park();
		}
		
	}
	
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
