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
package io.ecarf.core.cloud.impl.google;

import java.util.Set;

import com.google.common.collect.Sets;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public enum InstanceStatus {

	// Resources are being reserved for the instance. The instance isn't running yet.
	PROVISIONING, 
	// Resources have been acquired and the instance is being prepared for launch.
	STAGING, 
	// The instance is booting up or running. You should be able to ssh into the instance soon, though not immediately, after it enters this state.
	RUNNING, 
	//The instance is being stopped either due to a failure, or the instance being shut down. This is a temporary status and the instance will move to either PROVISIONING or TERMINATED.
	STOPPING, 
	// The instance either failed for some reason or was shutdown. This is a permanent status, and the only way to repair the instance is to delete and recreate it.
	TERMINATED; 
	
	public static final Set<String> IN_PROGRESS = Sets.newHashSet(PROVISIONING.toString(), 
			STAGING.toString(), STOPPING.toString());
}
