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
package io.ecarf.core.cloud;

import io.ecarf.core.cloud.impl.google.GoogleCloudService;

import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;

import com.google.common.collect.Lists;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TestUtils {
	
	public static final String TOKEN = "ya29.1.AADtN_VgVyFmkBiq7-e8XOo7IjVg9ht4zl_GEGS-NtbBPeW1RMyC7cbGr0QY_d7KqYm2USM";
	
	public static void prepare(GoogleCloudService service) {
		service.setAccessToken(TOKEN);
		service.setTokenExpire(DateUtils.addHours(new Date(), 1));
		service.setProjectId("ecarf-1000");
		service.setZone("us-central1-a");
		service.setInstanceId("ecarf-evm-1");
		service.setServiceAccount("default");
		service.setScopes(Lists.newArrayList("https://www.googleapis.com/auth/userinfo.email",
	        "https://www.googleapis.com/auth/compute",
	        "https://www.googleapis.com/auth/devstorage.full_control",
	        "https://www.googleapis.com/auth/bigquery"));
	}

}
