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

import io.ecarf.core.cloud.impl.google.GoogleCloudService;
import static io.ecarf.core.utils.Constants.*;

import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;


/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TestUtils {
	
	public static final String TOKEN = "ya29.1.AADtN_VTD0in7kcNyd-lUSj8CRSlBLqpfpiSrz-6nXI7wyf-EQZheKeGIYpX1vg";
	
	@SuppressWarnings("unchecked")
	public static void prepare(GoogleCloudService service) {
		service.setAccessToken(TOKEN);
		service.setTokenExpire(DateUtils.addHours(new Date(), 1));
		service.setProjectId(Config.getProperty(PROJECT_ID_KEY));
		service.setZone(Config.getProperty(ZONE_KEY));
		service.setInstanceId("ecarf-evm-1");
		service.setServiceAccount("default");
		service.setScopes(Config.getList(ACCESS_SCOPES_KEY));
	}

}
