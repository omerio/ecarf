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
import io.cloudex.cloud.impl.google.auth.CmdLineAuthenticationProvider;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Sets;


/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TestUtils {
	
	//public static final String TOKEN = "ya29.mAHnKvDvi-OOVB6tc87eAA6hA2H__cRTcaKUwPdgALpd3j3lZgkUTZK2OM3OezLesW84RR1_hoaYjA";
	
    /**
     * This uses the CmdLineAuthenticationProvider so there is no need to start a new
     * Compute Engine instance for the Coordinator
     * @param service
     * @throws IOException 
     */
	@SuppressWarnings("unchecked")
	public static void prepare(EcarfGoogleCloudServiceImpl service) throws IOException {
		//service.setAccessToken(TOKEN);
		//service.setTokenExpire(DateUtils.addHours(new Date(), 1));
		service.setProjectId(Config.getProperty(PROJECT_ID_KEY));
		service.setZone(Config.getProperty(ZONE_KEY));
		service.setInstanceId("ecarf-evm-1");
		service.setServiceAccount("default");
		service.setRemote(true);
		List<String> scopes = Config.getList(ACCESS_SCOPES_KEY);
		service.setScopes(scopes);
		CmdLineAuthenticationProvider provider = new CmdLineAuthenticationProvider();
		provider.setClientSecretsFile("/client_secret.json");
		provider.setScopes(Sets.newHashSet(scopes));
		service.setAuthenticationProvider(provider);
		service.init();
	}

}
