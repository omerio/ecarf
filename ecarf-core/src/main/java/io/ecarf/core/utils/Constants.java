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
package io.ecarf.core.utils;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Constants {
	
	/**
	 * 5 seconds before API operations are checked for completion
	 */
	public static final int API_RECHECK_DELAY = 5;
	
	public static final String GOOGLE = "google";
	
	public static final String AMAZON = "amazon";
	
	public static final String UTF8 = "UTF-8";
	
	public static final String GZIP_EXT = ".gz";
	
	public static final String APP_NAME = "ecarf";
	
	public static final String BINARY_CONTENT_TYPE = "application/octet-stream";
	
	public static final String GZIP_CONTENT_TYPE = "application/x-gzip";
	
	public static final String DOT_JSON = ".json";
	
	public static final String NODE_TERMS = "node_terms_";
	
	public static final String DOT_TERMS = ".terms";
	
	public static final String DOT_INF = ".inf";
	
	public static final String DOT_LOG = ".log";
	
	public static final String OUTPUT = "output_";
	
	public static final String OUT_FILE_SUFFIX = "_out.";
	
	public static final String SCHEMA_TERMS_JSON = "schema_terms.json";
	
	public static final String COMPRESSED_N_TRIPLES = ".nt.gz";
	
	public static final String PROCESSED_FILES = OUT_FILE_SUFFIX + "gz";
	
	public static final String EVM_EXCEPTION = "EVM Exception: ";
	
	public static final int GZIP_BUF_SIZE = 1024 * 1024 * 100;
	
	// config keys
	public static final String TERM_NEW_BIN_KEY = "ecarf.io.term.partition.new.bin.percentage";
	public static final String FILE_BIN_CAP_KEY = "ecarf.io.file.partition.new.bin.weight";
	public static final String API_DELAY_KEY = "ecarf.io.api.check.delay.seconds";
	public static final String IMAGE_ID_KEY  = "ecarf.io.vm.image.id";
	public static final String NETWORK_ID_KEY = "ecarf.io.vm.network.id";
	public static final String VM_TYPE_KEY = "ecarf.io.vm.type";
	public static final String PROJECT_ID_KEY = "ecarf.io.vm.project.id";
	public static final String ZONE_KEY = "ecarf.io.vm.zone";
	public static final String STARTUP_SCRIPT_KEY = "ecarf.io.vm.startup.script";
	public static final String ACCESS_SCOPES_KEY = "ecarf.io.vm.access.scopes";
	public static final String REASON_SLEEP_KEY = "ecarf.io.reasoning.sleep.time";
	public static final String REASON_RETRY_KEY = "ecarf.io.reasoning.retry.times";
	public static final String OUTPUT_FILE_KEY = "ecarf.io.output.log.file";
	public static final String OUTPUT_FOLDER_KEY = "ecarf.io.output.log.folder";
}
