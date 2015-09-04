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

import io.cloudex.framework.components.Processor;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The program p of the Ecarf framework
 * 
 * @author omerio
 *
 */
public class EcarfEvmTask {
	
	private final static Log log = LogFactory.getLog(EcarfEvmTask.class);
	


	/**
	 * 1- Read metadata
	 * 2- Compute with metadata
	 * 3- Update Status metadata
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String [] args) throws IOException {

	    Processor processor = null;

	    try {
	        
	        processor = new Processor.Builder(new EcarfGoogleCloudServiceImpl()).build();

	    } catch(IOException e) {
	        log.error("Failed to start the processor program", e);
	        throw e;
	    }


	    processor.run();
	}

	

}
