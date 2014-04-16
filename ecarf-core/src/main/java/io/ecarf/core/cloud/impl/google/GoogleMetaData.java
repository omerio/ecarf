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

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class GoogleMetaData {
	
	// metadata server urls
	public static final String METADATA_SERVER_URL = "http://metadata/computeMetadata/v1/";
	public static final String TOKEN_PATH = "instance/service-accounts/default/token";
	//private static final String SERVICE_ACCOUNT_PATH = "service-accounts/default/?recursive=true";
	public static final String INSTANCE_ALL_PATH = "instance/?recursive=true";
	//private static final String PROJECT_ALL_PATH = "project/?recursive=true";
	public static final String PROJECT_ID_PATH = "project/project-id";
	public static final String ATTRIBUTES_PATH = "instance/attributes/?recursive=true";
	public static final String WAIT_FOR_CHANGE = "&wait_for_change=true";
	
	// scopes
	public static final String DATASTORE_SCOPE = "https://www.googleapis.com/auth/datastore";
	
	public static final String RESOURCE_BASE_URL = "https://www.googleapis.com/compute/v1/projects/";
	public static final String NETWORK = "/global/networks/";
	public static final String ZONES = "/zones/";
	public static final String MACHINE_TYPES = "/machineTypes/";
	public static final String CENTO_IMAGE = "/centos-cloud/global/images/centos-6-v20140318";
	public static final String ACCESS_TOKEN = "access_token";
	public static final String EXPIRES_IN = "expires_in";
	public static final String PROJECT_ID = "projectId";
	public static final String ID = "id";
	public static final String HOSTNAME = "hostname";
	public static final String ZONE = "zone";
	public static final String ATTRIBUTES = "attributes";
	public static final String ITEMS = "items";
	public static final String FINGER_PRINT = "fingerprint";
	public static final String DONE = "DONE";
	public static final String EMAIL = "email";
	public static final String SCOPES = "scopes";
	public static final String SERVICE_ACCOUNTS = "serviceAccounts";
	public static final String DEFAULT = "default";
	public static final String IMAGE = "image";
	public static final String PERSISTENT = "PERSISTENT";
	public static final String MIGRATE = "MIGRATE";
	public static final String EXT_NAT = "External NAT";
	public static final String ONE_TO_ONE_NAT = "ONE_TO_ONE_NAT";
	public static final String STARTUP_SCRIPT = "startup-script";
	
	

}
