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

import static io.ecarf.core.utils.Constants.ACCESS_SCOPES_KEY;
import static io.ecarf.core.utils.Constants.PROJECT_ID_KEY;
import static io.ecarf.core.utils.Constants.ZONE_KEY;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;

import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;


/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TestUtils {
	
	public static final String TOKEN = "ya29.mAHnKvDvi-OOVB6tc87eAA6hA2H__cRTcaKUwPdgALpd3j3lZgkUTZK2OM3OezLesW84RR1_hoaYjA";
	
	@SuppressWarnings("unchecked")
	public static void prepare(EcarfGoogleCloudServiceImpl service) {
		service.setAccessToken(TOKEN);
		service.setTokenExpire(DateUtils.addHours(new Date(), 1));
		service.setProjectId(Config.getProperty(PROJECT_ID_KEY));
		service.setZone(Config.getProperty(ZONE_KEY));
		service.setInstanceId("ecarf-evm-1");
		service.setServiceAccount("default");
		service.setScopes(Config.getList(ACCESS_SCOPES_KEY));
	}

}
