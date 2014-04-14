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

import static io.ecarf.core.utils.Constants.AMAZON;
import static io.ecarf.core.utils.Constants.GOOGLE;
import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.impl.google.GoogleCloudService;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class EcarfCcvmTask {
	
	private final static Logger log = Logger.getLogger(EcarfCcvmTask.class.getName()); 
	
	private CloudService service;
	
	private VMMetaData metadata;
	
	
	public void run() throws IOException {
		
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
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String platform = GOOGLE;
		if(args.length > 0) {
			platform = args[0];
		}
		
		EcarfCcvmTask task = new EcarfCcvmTask();
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
