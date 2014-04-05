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

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.gzip.Callback;
import io.ecarf.core.gzip.GzipProcessor;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.gson.Gson;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class GoogleCloudService implements CloudService {
	
	private final static Logger log = Logger.getLogger(GoogleCloudService.class.getName()); 
	
	private static final String METADATA_SERVER_URL = "http://metadata/computeMetadata/v1/";
	
	private static final String TOKEN_PATH = "instance/service-accounts/default/token";
	
	private static final String INSTANCE_ALL_PATH = "instance/?recursive=true";
	
	private static final String PROJECT_ALL_PATH = "project/?recursive=true";
	
	private static final String PROJECT_ID_PATH = "project/projectId";

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	
	private static final int TOKEN_EXPIRE = 1;
	
	/** Global instance of the HTTP transport. */
	private HttpTransport httpTransport;
	
	private Storage storage;
	
	private Compute compute;
	
	private String accessToken;
	
	private Date tokenStart;
	
	private String projectId;
	
	private String zone;
	
	private Long instanceId;
	
	/**
	 * Perform initialization before
	 * this cloud service is used
	 * @throws IOException 
	 */
	public void inti() throws IOException {
		Map<String, String> attributes;
		this.projectId = getMetaData(PROJECT_ID_PATH);
		Map<String, Object> metaData = Utils.jsonToMap(getMetaData(INSTANCE_ALL_PATH));
		attributes = (Map<String, String>) metaData.get(ATTRIBUTES);
		this.zone = (String) metaData.get(ZONE);
		this.instanceId = (Long) metaData.get(ID);
		this.authorise();
		this.getHttpTransport();
		this.getCompute();
		this.getStorage();
		
	}
	
	
	/**
	 * Call the metadata server 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private String getMetaData(String path) throws IOException {
		log.fine("Retrieving metadata from server, path: " + path);
		URL metadata = new URL(METADATA_SERVER_URL + path);
		HttpURLConnection con = (HttpURLConnection) metadata.openConnection();
 
		// optional default is GET
		//con.setRequestMethod("GET");
 
		//add request header
		con.setRequestProperty("X-Google-Metadata-Request", "true");
 
		int responseCode = con.getResponseCode();
		
		StringBuilder response = new StringBuilder();
		
		if(responseCode == 200) {
			try(BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
			}
		} else {
			String msg = "Metadata server responded with status code: " + responseCode;
			log.severe(msg);
			throw new IOException(msg);
		}
		log.fine("Successfully retrieved metadata from server");
		
		return response.toString();
	}

	/**
	 * Retrieves a service account access token from the metadata server, the response has the format
	 * {
		  "access_token":"ya29.AHES6ZRN3-HlhAPya30GnW_bHSb_QtAS08i85nHq39HE3C2LTrCARA",
		  "expires_in":3599,
		  "token_type":"Bearer"
		}
	 * @throws IOException 
	 * @see https://developers.google.com/compute/docs/authentication
	 */
	@SuppressWarnings({ "unused", "unchecked" })
	private void authorise() throws IOException {
		
		log.fine("Refreshing OAuth token from metadata server");
		this.accessToken = Utils.getStringPropertyFromJson(getMetaData(TOKEN_PATH), ACCESS_TOKEN);
		this.tokenStart = new Date();
		log.fine("Successfully refreshed OAuth token from metadata server");
	
	}
	
	/**
	 * Create a new instance of the HTTP transport
	 * @return
	 * @throws GeneralSecurityException
	 * @throws IOException
	 */
	private HttpTransport getHttpTransport() throws IOException {
		if(httpTransport == null) {
			try {
				httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			} catch (GeneralSecurityException e) {
				log.log(Level.SEVERE, "failed to create transport", e);
				throw new IOException(e);
			}
		}
		
		return httpTransport;
	}
	
	/**
	 * Get the token and also check if it's expired then request a new one
	 * @return
	 * @throws IOException
	 */
	private String getOAuthToken() throws IOException {
		Date now = new Date();
		if(now.after(DateUtils.addHours(tokenStart, TOKEN_EXPIRE))) {
			this.authorise();
		}
		return this.accessToken;
	}
	
	/**
	 * Create a compute API client instance
	 * @return
	 * @throws IOException 
	 * @throws GeneralSecurityException 
	 */
	private Compute getCompute() throws IOException {
		if(this.compute == null) {
			this.compute = new Compute.Builder(getHttpTransport(), JSON_FACTORY, null)
				.setApplicationName(Constants.APP_NAME).build();
		}
		return this.compute;
	}
	
	/**
	 * Create a storage API client instance
	 * @return
	 * @throws IOException 
	 * @throws GeneralSecurityException 
	 */
	private Storage getStorage() throws IOException {
		if(this.storage == null) {
			this.storage = new  Storage.Builder(getHttpTransport(), JSON_FACTORY, null)
				.setApplicationName(Constants.APP_NAME).build();
		}
		return this.storage;
	}
	
	/**
	 * Create a bucket on the mass cloud storage
	 * @param bucket
	 * @throws IOException 
	 */
	public void createCloudStorageBucket(String bucket, String location) throws IOException {

		Storage.Buckets.Insert insertBucket = storage.buckets()
				.insert(this.projectId, new Bucket().setName(bucket).setLocation(location)
				// .setDefaultObjectAcl(ImmutableList.of(
				// new ObjectAccessControl().setEntity("allAuthenticatedUsers").setRole("READER")))
				).setOauthToken(this.getOAuthToken());
		try {
			@SuppressWarnings("unused")
			Bucket createdBucket = insertBucket.execute();
		} catch (GoogleJsonResponseException e) {
			GoogleJsonError error = e.getDetails();
			if (error.getCode() == HTTP_CONFLICT
					&& error.getMessage().contains("You already own this bucket.")) {
				log.info(bucket + " already exists!");
			} else {
				throw e;
			}
		}
	}
	
	/**
	 * Convert the provided file to a format that can be imported to the Cloud Database
	 * 
	 * @param filename
	 * @return
	 * @throws IOException 
	 */
	public String prepareForCloudDatabaseImport(String filename) throws IOException {
		/*String outFilename = new StringBuilder(FileUtils.TEMP_FOLDER)
			.append(File.separator).append("out_").append(filename).toString();*/
		GzipProcessor processor = new GzipProcessor(filename);
		
		String outFilename = processor.process(new Callback() {

			@Override
			public String process(String line) {
				String[] terms = TripleUtils.parseTriple(line);
				String outLine = null;
				if(terms != null) {
					for(int i = 0; i < terms.length; i++) {
						// bigquery requires data to be properly escaped
						terms[i] = StringEscapeUtils.escapeCsv(terms[i]);
					}
					outLine = StringUtils.join(terms, ',');
				}
				return outLine;
			}
			
		});
		
		return outFilename;
	}
	

}
