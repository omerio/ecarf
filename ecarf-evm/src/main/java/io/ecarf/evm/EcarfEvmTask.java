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
import io.ecarf.core.cloud.impl.google.GoogleCloudService;

/**
 * The program p of the Ecarf framework
 * 
 * @author omerio
 *
 */
public class EcarfEvmTask {
	
	private CloudService service;
	
	/**
	 * 
	 */
	public void run() {
		while(true) {
			
			// Load task
			// read the files from http:// or from gs://
			// download files locally (gziped)
			// read through the files counting the relevant terms and rewriting into bigquery format (comma separated)
			
		}
	}
	
	/**
	 * @param service the service to set
	 */
	public void setService(CloudService service) {
		this.service = service;
	}

	/**
	 * 1- Read metadata
	 * 2- Compute with metadata
	 * 3- Update Status metadata
	 * 
	 * @param args
	 */
	public static void main(String [] args) {
		String platform = GOOGLE;
		if(args.length > 0) {
			platform = args[0];
		}
		
		EcarfEvmTask task = new EcarfEvmTask();
		
		switch(platform) {
		case GOOGLE:
			task.setService(new GoogleCloudService());
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
