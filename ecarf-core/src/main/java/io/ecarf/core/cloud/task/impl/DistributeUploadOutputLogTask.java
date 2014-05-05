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
package io.ecarf.core.cloud.task.impl;

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.task.CommonTask;
import io.ecarf.core.cloud.types.TaskType;
import io.ecarf.core.cloud.types.VMStatus;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.client.util.Lists;

/**
 * Read a list of files from cloud storage and based on their size split them
 * using a bin packing algorithm
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DistributeUploadOutputLogTask extends CommonTask {


	public DistributeUploadOutputLogTask(VMMetaData metadata, CloudService cloud) {
		super(metadata, cloud);
	}

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {

		log.info("Distribute upload output logs");

		List<String> activeNodes = (this.input.getNodes() != null) ? 
				Lists.newArrayList(this.input.getNodes()) : new ArrayList<String>();

		String zoneId = this.input.getZoneId();

		if(!activeNodes.isEmpty()) {
			
			for(String instanceId: activeNodes) {	

				VMMetaData metaData = this.cloud.getEcarfMetaData(instanceId, zoneId);
				String fingerprint = metaData.getFingerprint();
				metaData = this.getMetadata();
				metaData.setFingerprint(fingerprint);
				this.cloud.updateInstanceMetadata(metaData, zoneId, instanceId, true);
			}

			// wait for the VMs to finish their loading
			for(String instanceId: activeNodes) {	
				boolean ready = false;

				do {
					Utils.block(Utils.getApiRecheckDelay());

					VMMetaData metaData = this.cloud.getEcarfMetaData(instanceId, zoneId);
					ready = VMStatus.READY.equals(metaData.getVMStatus());
					// TODO status can be error ERROR
					if(VMStatus.ERROR.equals(metaData.getVMStatus())) {
						// for now we are throwing an exception, in the future need to return a status 
						// so tasks can be retried
						throw Utils.exceptionFromEcarfError(metaData, instanceId);

					}

				} while (!ready);
			}

		}

		log.info("Successfully processed distribute upload logs");
	}

	/**
	 * Prepare the vm metadata for reasoning
	 * @param terms
	 * @return
	 */
	private VMMetaData getMetadata() {
		VMMetaData metaData = new VMMetaData();

		metaData.addValue(VMMetaData.ECARF_TASK, TaskType.UPLOAD_LOGS.toString())
		.addValue(VMMetaData.ECARF_BUCKET, this.input.getBucket());

		return metaData;
	}


}
