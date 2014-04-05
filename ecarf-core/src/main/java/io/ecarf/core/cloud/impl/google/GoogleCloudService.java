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

import static io.ecarf.core.cloud.impl.google.GoogleMetaData.ACCESS_TOKEN;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.ATTRIBUTES;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.HOSTNAME;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.ZONE;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.impl.google.storage.DownloadProgressListener;
import io.ecarf.core.cloud.impl.google.storage.UploadProgressListener;
import io.ecarf.core.gzip.GzipProcessor;
import io.ecarf.core.gzip.GzipProcessorCallback;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Callback;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.time.DateUtils;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;

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
	
	private static final String PROJECT_ID_PATH = "project/project-id";

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
	
	private String instanceId;
	
	/**
	 * Perform initialization before
	 * this cloud service is used
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Map<String, String> inti() throws IOException {
		Map<String, String> attributes;
		this.projectId = getMetaData(PROJECT_ID_PATH);
		Map<String, Object> metaData = Utils.jsonToMap(getMetaData(INSTANCE_ALL_PATH));
		attributes = (Map<String, String>) metaData.get(ATTRIBUTES);
		
		// strangely zone looks like this: "projects/315344313954/zones/us-central1-a"
		this.zone = (String) metaData.get(ZONE);
		this.zone = StringUtils.substringAfterLast(this.zone, "/");
		
		// the name isn't returned!, but the hostname looks like this:
		// "ecarf-evm-1.c.ecarf-1000.internal"
		this.instanceId = (String) metaData.get(HOSTNAME);
		this.instanceId = StringUtils.substringBefore(this.instanceId, ".");
		
		this.authorise();
		this.getHttpTransport();
		this.getCompute();
		this.getStorage();
		log.info("Successfully initialized Google Cloud Service: " + this);
		return attributes;
		
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
	@Override
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
	 * Upload the provided file into cloud storage
	 * @param filename
	 * @param bucket
	 * @throws IOException 
	 */
	@Override
	public void uploadFileToCloudStorage(String filename, String bucket, Callback callback) throws IOException {
		
		FileInputStream fileStream = new FileInputStream(filename);
		
		InputStreamContent mediaContent = new InputStreamContent(Constants.BINARY_CONTENT_TYPE, fileStream);
		
		// Not strictly necessary, but allows optimization in the cloud.
		mediaContent.setLength(fileStream.available());

		Storage.Objects.Insert insertObject =
				getStorage().objects().insert(bucket, null, mediaContent);

		insertObject.getMediaHttpUploader().setProgressListener(
				new UploadProgressListener(callback)).setDisableGZipContent(true);
		// For small files, you may wish to call setDirectUploadEnabled(true), to
		// reduce the number of HTTP requests made to the server.
		if (mediaContent.getLength() > 0 && mediaContent.getLength() <=  2 * FileUtils.ONE_MB /* 2MB */) {
			insertObject.getMediaHttpUploader().setDirectUploadEnabled(true);
		}
		
		insertObject.execute();
	}
	
	/**
	 * Download an object from cloud storage to a file
	 * @param object
	 * @param outFile
	 * @param bucket
	 * @param callback
	 * @throws IOException
	 */
	@Override
	public void downloadObjectFromCloudStorage(String object, String outFile, 
			String bucket, Callback callback) throws IOException {
		
		FileOutputStream out = new FileOutputStream(outFile);
	
		Storage.Objects.Get getObject =
				getStorage().objects().get(bucket, object);

		
		getObject.getMediaHttpDownloader().setDirectDownloadEnabled(true)
			.setProgressListener(new DownloadProgressListener(callback));
		
		getObject.executeMediaAndDownloadTo(out);
		
	}
	
	/**
	 * Convert the provided file to a format that can be imported to the Cloud Database
	 * 
	 * @param filename
	 * @return
	 * @throws IOException 
	 */
	@Override
	public String prepareForCloudDatabaseImport(String filename) throws IOException {
		/*String outFilename = new StringBuilder(FileUtils.TEMP_FOLDER)
			.append(File.separator).append("out_").append(filename).toString();*/
		GzipProcessor processor = new GzipProcessor(filename);
		
		String outFilename = processor.process(new GzipProcessorCallback() {

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
	
	/**
	 * Update the meta data of the current instance
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	@Override
	public void updateInstanceMetadata(String key, String value) throws IOException {
		this.updateInstanceMetadata(key, value, this.zone, this.instanceId);
	}
	
	/**
	 * Update the meta data of the current instance
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	@Override
	public void updateInstanceMetadata(String key, String value, String zone, String instanceId) throws IOException {
		Metadata metadata = new Metadata();
		metadata.set(key, value);
		this.getCompute().instances().setMetadata(projectId, zone, instanceId, metadata).execute();
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return new ToStringBuilder(this).
				append("projectId", this.projectId).
				append("instanceId", this.instanceId).
				append("zone", this.zone).
				append("token", this.accessToken).
				append("tokenStart", this.tokenStart).
				toString();
	}


	/**
	 * @param accessToken the accessToken to set
	 */
	protected void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}


	/**
	 * @param projectId the projectId to set
	 */
	protected void setProjectId(String projectId) {
		this.projectId = projectId;
	}

}
