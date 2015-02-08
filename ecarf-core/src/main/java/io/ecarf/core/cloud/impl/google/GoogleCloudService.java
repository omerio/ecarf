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
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.ATTRIBUTES_PATH;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.CLOUD_STORAGE_PREFIX;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.CREATE_IF_NEEDED;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.CREATE_NEVER;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.DATASTORE_SCOPE;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.DEFAULT;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.DISK_TYPES;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.DONE;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.EMAIL;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.EXPIRES_IN;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.EXT_NAT;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.HOSTNAME;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.INSTANCE_ALL_PATH;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.MACHINE_TYPES;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.METADATA_SERVER_URL;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.MIGRATE;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.NETWORK;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.NOT_FOUND;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.ONE_TO_ONE_NAT;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.PERSISTENT;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.PROJECT_ID_PATH;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.RESOURCE_BASE_URL;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.SCOPES;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.SERVICE_ACCOUNTS;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.TOKEN_PATH;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.WAIT_FOR_CHANGE;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.WRITE_APPEND;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.ZONE;
import static io.ecarf.core.cloud.impl.google.GoogleMetaData.ZONES;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.cloud.VMConfig;
import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.impl.google.storage.DownloadProgressListener;
import io.ecarf.core.cloud.impl.google.storage.UploadProgressListener;
import io.ecarf.core.compress.NTripleGzipCallback;
import io.ecarf.core.compress.NTripleGzipProcessor;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Callback;
import io.ecarf.core.utils.Config;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.FutureTask;
import io.ecarf.core.utils.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Data;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.JobStatistics3;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.Compute.Instances.Insert;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.AttachedDiskInitializeParams;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.Metadata.Items;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Operation.Error.Errors;
import com.google.api.services.compute.model.Scheduling;
import com.google.api.services.compute.model.ServiceAccount;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class GoogleCloudService implements CloudService {

	private final static Log log = LogFactory.getLog(GoogleCloudService.class);

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	/** Global instance of the HTTP transport. */
	private HttpTransport httpTransport;

	private Storage storage;

	private Compute compute;

	private Bigquery bigquery;

	// service account access token retrieved from the metadata server
	private String accessToken;

	// the expiry time of the token
	private Date tokenExpire;

	private String projectId;

	private String zone;

	private String instanceId;

	private String serviceAccount;

	private List<String> scopes;

	/**
	 * Perform initialization before
	 * this cloud service is used
	 * @throws IOException 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public VMMetaData inti() throws IOException {
		Map<String, Object> attributes;
		this.projectId = getMetaData(PROJECT_ID_PATH);
		Map<String, Object> metaData = Utils.jsonToMap(getMetaData(INSTANCE_ALL_PATH));
		attributes = (Map<String, Object>) metaData.get(ATTRIBUTES);

		// strangely zone looks like this: "projects/315344313954/zones/us-central1-a"
		this.zone = (String) metaData.get(ZONE);
		this.zone = StringUtils.substringAfterLast(this.zone, "/");

		// the name isn't returned!, but the hostname looks like this:
		// "ecarf-evm-1.c.ecarf-1000.internal"
		this.instanceId = (String) metaData.get(HOSTNAME);
		this.instanceId = StringUtils.substringBefore(this.instanceId, ".");

		// get the default service account
		Map<String, Object> serviceAccountConfig = ((Map) ((Map) metaData.get(SERVICE_ACCOUNTS)).get(DEFAULT));
		this.serviceAccount = (String) serviceAccountConfig.get(EMAIL);
		this.scopes = (List) serviceAccountConfig.get(SCOPES);
		// add the datastore scope as well
		this.scopes.add(DATASTORE_SCOPE);

		this.authorise();
		this.getHttpTransport();
		this.getCompute();
		this.getStorage();

		Instance instance = this.getInstance(instanceId, zone);
		String fingerprint = instance.getMetadata().getFingerprint();

		log.info("Successfully initialized Google Cloud Service: " + this);
		return new VMMetaData(attributes, fingerprint);

	}


	/**
	 * Call the metadata server, this returns details for the current instance not for
	 * different instances. In order to retrieve the meta data of different instances
	 * we just use the compute api, see getInstance
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private String getMetaData(String path) throws IOException {
		log.info("Retrieving metadata from server, path: " + path);
		URL metadata = new URL(METADATA_SERVER_URL + path);
		HttpURLConnection con = (HttpURLConnection) metadata.openConnection();

		// optional default is GET
		//con.setRequestMethod("GET");

		//add request header
		con.setRequestProperty("Metadata-Flavor", "Google");
		
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
			log.error(msg);
			throw new IOException(msg);
		}
		log.info("Successfully retrieved metadata from server");

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

		log.debug("Refreshing OAuth token from metadata server");
		Map<String, Object> token = Utils.jsonToMap(getMetaData(TOKEN_PATH));
		this.accessToken = (String) token.get(ACCESS_TOKEN);

		Double expiresIn = (Double) token.get(EXPIRES_IN);
		this.tokenExpire = DateUtils.addSeconds(new Date(), expiresIn.intValue());

		log.debug("Successfully refreshed OAuth token from metadata server");

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
				log.error("failed to create transport", e);
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

		if((this.tokenExpire == null) || (new Date()).after(this.tokenExpire)) {
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
	 * Create a bigquery API client instance
	 * @return
	 * @throws IOException 
	 * @throws GeneralSecurityException 
	 */
	private Bigquery getBigquery() throws IOException {
		if(this.bigquery == null) {
			this.bigquery = new Bigquery.Builder(getHttpTransport(), JSON_FACTORY, null)
			.setApplicationName(Constants.APP_NAME).build();
		}
		return this.bigquery;
	}

	/**
	 * Create a storage API client instance
	 * @return
	 * @throws IOException 
	 * @throws GeneralSecurityException 
	 */
	private Storage getStorage() throws IOException {
		if(this.storage == null) {
			this.storage = new  Storage.Builder(getHttpTransport(), JSON_FACTORY, null/*, new HttpRequestInitializer() {
				public void initialize(HttpRequest request) {
					request.setUnsuccessfulResponseHandler(new RedirectHandler());
				}
			}*/)
			.setApplicationName(Constants.APP_NAME).build();
		}
		return this.storage;
	}

	//------------------------------------------------- Storage -------------------------------
	/**
	 * Create a bucket on the mass cloud storage
	 * @param bucket
	 * @throws IOException 
	 */
	@Override
	public void createCloudStorageBucket(String bucket, String location) throws IOException {

		Storage.Buckets.Insert insertBucket = this.getStorage().buckets()
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
	 * THis is what Google returns:
	 * CONFIG: {
		 "kind": "storage#object",
		 "id": "ecarf/umbel_links.nt_out.gz/1397227987451000",
		 "selfLink": "https://www.googleapis.com/storage/v1beta2/b/ecarf/o/umbel_links.nt_out.gz",
		 "name": "umbel_links.nt_out.gz",
		 "bucket": "ecarf",
		 "generation": "1397227987451000",
		 "metageneration": "1",
		 "contentType": "application/x-gzip",
		 "updated": "2014-04-11T14:53:07.339Z",
		 "storageClass": "STANDARD",
		 "size": "8474390",
		 "md5Hash": "UPhXcZZGbD9198OhQcdnvQ==",
		 "mediaLink": "https://www.googleapis.com/storage/v1beta2/b/ecarf/o/umbel_links.nt_out.gz?generation=1397227987451000&alt=media",
		 "owner": {
		  "entity": "user-00b4903a97e56638621f0643dc282444442a11b19141d3c7b425c4d17895dcf6",
		  "entityId": "00b4903a97e56638621f0643dc282444442a11b19141d3c7b425c4d17895dcf6"
		 },
		 "crc32c": "3twYkA==",
		 "etag": "CPj48u7X2L0CEAE="
		}
	 * @param filename
	 * @param bucket
	 * @throws IOException 
	 */
	@Override
	public io.ecarf.core.cloud.storage.StorageObject uploadFileToCloudStorage(String filename, String bucket, Callback callback) throws IOException {

		FileInputStream fileStream = new FileInputStream(filename);
		String contentType;
		boolean gzipDisabled;

		if(GzipUtils.isCompressedFilename(filename)) {
			contentType = Constants.GZIP_CONTENT_TYPE;
			gzipDisabled = true;

		} else {
			contentType = Files.probeContentType((new File(filename)).toPath());
			if(contentType == null) {
				contentType = Constants.BINARY_CONTENT_TYPE;
			}
			gzipDisabled = false;
		}

		InputStreamContent mediaContent = new InputStreamContent(contentType, fileStream);

		// Not strictly necessary, but allows optimization in the cloud.
		mediaContent.setLength(fileStream.available());

		Storage.Objects.Insert insertObject =
				getStorage().objects().insert(bucket, 
						new StorageObject().setName(StringUtils.substringAfterLast(filename, Utils.PATH_SEPARATOR)), 
						mediaContent).setOauthToken(this.getOAuthToken());

		insertObject.getMediaHttpUploader().setProgressListener(
				new UploadProgressListener(callback)).setDisableGZipContent(gzipDisabled);
		// For small files, you may wish to call setDirectUploadEnabled(true), to
		// reduce the number of HTTP requests made to the server.
		if (mediaContent.getLength() > 0 && mediaContent.getLength() <=  4 * FileUtils.ONE_MB /* 2MB */) {
			insertObject.getMediaHttpUploader().setDirectUploadEnabled(true);
		}

		StorageObject object = insertObject.execute();
		
		return this.getEcarfStorageObject(object, bucket);
	}

	/**
	 * Upload a file to cloud storage and block until it's uploaded
	 * @param filename
	 * @param bucket
	 * @throws IOException
	 */
	@Override
	public io.ecarf.core.cloud.storage.StorageObject uploadFileToCloudStorage(String filename, String bucket) throws IOException {

		final FutureTask task = new FutureTask();

		Callback callback = new Callback() {
			@Override
			public void execute() {
				log.info("Upload complete");
				task.setDone(true);
			}
		};

		io.ecarf.core.cloud.storage.StorageObject storageObject = this.uploadFileToCloudStorage(filename, bucket, callback);

		// wait for the upload to finish
		while(!task.isDone()) {
			Utils.block(5);
		}
		
		return storageObject;

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

		log.info("Downloading cloud storage file " + object + ", to: " + outFile);

		FileOutputStream out = new FileOutputStream(outFile);

		Storage.Objects.Get getObject =
				getStorage().objects().get(bucket, object).setOauthToken(this.getOAuthToken());

		getObject.getMediaHttpDownloader().setDirectDownloadEnabled(true)
		.setProgressListener(new DownloadProgressListener(callback));

		getObject.executeMediaAndDownloadTo(out);

	}

	/**
	 * Download an object from cloud storage to a file, this method will block until the file is downloaded
	 * @param object
	 * @param outFile
	 * @param bucket
	 * @param callback
	 * @throws IOException
	 */
	@Override
	public void downloadObjectFromCloudStorage(String object, final String outFile, 
			String bucket) throws IOException {

		//final Thread currentThread = Thread.currentThread();
		final FutureTask task = new FutureTask();

		Callback callback = new Callback() {
			@Override
			public void execute() {
				log.info("Download complete, file saved to: " + outFile);
				//LockSupport.unpark(currentThread);
				task.setDone(true);
			}
		};

		this.downloadObjectFromCloudStorage(object, outFile, bucket, callback);

		// wait for the download to take place
		//LockSupport.park();
		while(!task.isDone()) {
			Utils.block(5);
		}

	}

	@Override
	public List<io.ecarf.core.cloud.storage.StorageObject> listCloudStorageObjects(String bucket) throws IOException {
		List<io.ecarf.core.cloud.storage.StorageObject> objects = new ArrayList<>();
		Storage.Objects.List listObjects =  
				getStorage().objects().list(bucket).setOauthToken(this.getOAuthToken());
		// we are not paging, just get everything
		Objects cloudObjects = listObjects.execute();

		for (StorageObject cloudObject : cloudObjects.getItems()) {
			// Do things!
			io.ecarf.core.cloud.storage.StorageObject object = this.getEcarfStorageObject(cloudObject, bucket);
			objects.add(object);
		}
		return objects;
	}
	
	/**
	 * Create an ecarf storage object from a Google com.google.api.services.storage.model.StorageObject
	 * @param cloudObject
	 * @param bucket
	 * @return
	 */
	private io.ecarf.core.cloud.storage.StorageObject getEcarfStorageObject(StorageObject cloudObject, String bucket) {
		io.ecarf.core.cloud.storage.StorageObject object = 
				new io.ecarf.core.cloud.storage.StorageObject();
		object.setContentType(cloudObject.getContentType());
		object.setDirectLink(cloudObject.getSelfLink());
		object.setName(cloudObject.getName());
		object.setSize(cloudObject.getSize());
		object.setUri(CLOUD_STORAGE_PREFIX + bucket + "/" + cloudObject.getName());
		return object;
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
		return this.prepareForCloudDatabaseImport(filename, null);
	}

	/**
	 * Convert the provided file to a format that can be imported to the Cloud Database
	 * 
	 * @param filename
	 * @return
	 * @throws IOException 
	 */
	@Override
	public String prepareForCloudDatabaseImport(String filename, final TermCounter counter) throws IOException {
		/*String outFilename = new StringBuilder(FileUtils.TEMP_FOLDER)
			.append(File.separator).append("out_").append(filename).toString();*/
		NTripleGzipProcessor processor = new NTripleGzipProcessor(filename);

		String outFilename = processor.process(new NTripleGzipCallback() {

			@Override
			public String process(String [] terms) {

				if(counter != null) {
					counter.count(terms);
				}

				for(int i = 0; i < terms.length; i++) {
					// bigquery requires data to be properly escaped
					terms[i] = StringEscapeUtils.escapeCsv(terms[i]);
				}

				return StringUtils.join(terms, ',');
			}

		});

		return outFilename;
	}

	//------------------------------------------------- Compute -------------------------------

	/**
	 * 
	 * @param instanceId
	 * @param zoneId
	 * @return
	 * @throws IOException
	 */
	private Instance getInstance(String instanceId, String zoneId) throws IOException {
		return this.getCompute().instances().get(this.projectId, 
				zoneId != null ? zoneId : this.zone, instanceId)
				.setOauthToken(this.getOAuthToken())
				.execute();
	}


	/**
	 * Get the meta data of the current instance, this will simply call the metadata server.
	 * Wait for change will block until there is a change
	 * @return
	 * @throws IOException 
	 */
	@Override
	public VMMetaData getEcarfMetaData(boolean waitForChange) throws IOException {
		String metaData = this.getMetaData(ATTRIBUTES_PATH + (waitForChange ? WAIT_FOR_CHANGE : ""));

		Map<String, Object> attributes = Utils.jsonToMap(metaData);
		Instance instance = this.getInstance(instanceId, zone);
		String fingerprint = instance.getMetadata().getFingerprint();
		return new VMMetaData(attributes, fingerprint);

	}

	/**
	 * Get the meta data for the provided instance id
	 * @param instanceId
	 * @param zoneId
	 * @return
	 * @throws IOException 
	 */
	@Override
	public VMMetaData getEcarfMetaData(String instanceId, String zoneId) throws IOException {
		Instance instance = this.getInstance(instanceId != null ? instanceId : this.instanceId, 
				zoneId != null ? zoneId : this.zone);
		List<Items> items = instance.getMetadata().getItems();
		Map<String, Object> attributes = new HashMap<>();
		for(Items item: items) {
			attributes.put(item.getKey(), item.getValue());
		}
		String fingerprint = instance.getMetadata().getFingerprint();
		return new VMMetaData(attributes, fingerprint);
	}

	/**
	 *
	 * Update the meta data of the current instance
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	@Override
	public void updateInstanceMetadata(VMMetaData metaData) throws IOException {
		this.updateInstanceMetadata(metaData, this.zone, this.instanceId, true);
	}

	/**
	 * 
	 * Update the meta data of the the provided instance
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	@Override
	public void updateInstanceMetadata(VMMetaData metaData, 
			String zoneId, String instanceId, boolean block) throws IOException {

		Metadata metadata = this.getMetaData(metaData);

		Operation operation = this.getCompute().instances().setMetadata(projectId, zoneId, instanceId, metadata)
				.setOauthToken(this.getOAuthToken()).execute();

		log.info("Successuflly initiated operation: " + operation);

		// shall we wait until the operation is complete?
		if(block) {
			this.blockOnOperation(operation, zoneId);

			// update the fingerprint of the current metadata
			Instance instance = this.getInstance(instanceId, zoneId);
			metaData.setFingerprint(instance.getMetadata().getFingerprint());
		}
	}

	/**
	 * Wait until the provided operation is done, if the operation returns an error then an IOException 
	 * will be thrown
	 * @param operation
	 * @param zoneId
	 * @throws IOException
	 */
	private void blockOnOperation(Operation operation, String zoneId) throws IOException {
		do {
			// sleep for 10 seconds before checking the operation status
			Utils.block(Utils.getApiRecheckDelay());

			operation = this.getCompute().zoneOperations().get(projectId, zoneId, operation.getName())
					.setOauthToken(this.getOAuthToken()).execute();

			// check if the operation has actually failed
			if((operation.getError() != null) && (operation.getError().getErrors() != null)) {
				Errors error = operation.getError().getErrors().get(0);
				throw new IOException("Operation failed: " + error.getCode() + " - " + error.getMessage());
			}

		} while(!DONE.endsWith(operation.getStatus()));
	}

	/**
	 * Create VM instances, optionally block until all are created. If any fails then the returned flag is false
	 * 
	 *  body = {
    'name': NEW_INSTANCE_NAME,
    'machineType': <fully-qualified-machine-type-url>,
    'networkInterfaces': [{
      'accessConfigs': [{
        'type': 'ONE_TO_ONE_NAT',
        'name': 'External NAT'
       }],
      'network': <fully-qualified-network-url>
    }],
    'disk': [{
       'autoDelete': 'true',
       'boot': 'true',
       'type': 'PERSISTENT',
       'initializeParams': {
          'diskName': 'my-root-disk',
          'sourceImage': '<fully-qualified-image-url>',
       }
     }]
  }
	 * @param config
	 * @param block
	 * @throws IOException
	 */
	@Override
	public boolean startInstance(List<VMConfig> configs, boolean block) throws IOException {

		for(VMConfig config: configs) {
			log.info("Creating VM for config: " + config);
			String zoneId = config.getZoneId();
			zoneId = zoneId != null ? zoneId : this.zone;
			Instance content = new Instance();

			// Instance config
			content.setMachineType(RESOURCE_BASE_URL + 
					this.projectId + ZONES + zoneId + MACHINE_TYPES+ config.getVmType());
			content.setName(config.getInstanceId());
			//content.setZone(zoneId);
			// startup script
			if(StringUtils.isNoneBlank(config.getStartupScript())) {
				config.getMetaData().addValue(GoogleMetaData.STARTUP_SCRIPT, config.getStartupScript());
			}
			content.setMetadata(this.getMetaData(config.getMetaData()));

			// service account
			ServiceAccount sa = new ServiceAccount();
			sa.setEmail(this.serviceAccount).setScopes(this.scopes);
			content.setServiceAccounts(Lists.newArrayList(sa));

			// network
			NetworkInterface inf = new NetworkInterface(); 
			inf.setNetwork(RESOURCE_BASE_URL + 
					this.projectId + NETWORK + config.getNetworkId());
			AccessConfig accessConf = new AccessConfig();
			accessConf.setType(ONE_TO_ONE_NAT).setName(EXT_NAT);
			inf.setAccessConfigs(Lists.newArrayList(accessConf));
			content.setNetworkInterfaces(Lists.newArrayList(inf));

			// scheduling
			Scheduling scheduling = new Scheduling();
			scheduling.setAutomaticRestart(false);
			scheduling.setOnHostMaintenance(MIGRATE);
			content.setScheduling(scheduling);

			// Disk
			AttachedDisk disk = new AttachedDisk();

			AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
			params.setDiskName(config.getInstanceId());
			params.setSourceImage(RESOURCE_BASE_URL + config.getImageId());
			//params.setDiskSizeGb(diskSizeGb)

			disk.setAutoDelete(true).setBoot(true)
			.setDeviceName(config.getInstanceId())
			.setType(PERSISTENT)
			.setInitializeParams(params);
			
			if(StringUtils.isNotBlank(config.getDiskType())) {
				// standard or SSD based disks
				params.setDiskType(RESOURCE_BASE_URL + 
					this.projectId + ZONES + zoneId + DISK_TYPES + config.getDiskType());
			}

			content.setDisks(Lists.newArrayList(disk));

			Insert insert = this.getCompute().instances()
					.insert(this.projectId, zoneId, content)
					.setOauthToken(this.getOAuthToken());

			Operation operation = insert.execute();
			log.info("Successuflly initiated operation: " + operation);
		}

		boolean success = true;

		// we have to block until all instances are provisioned
		if(block) {

			for(VMConfig config: configs) {
				String status = InstanceStatus.PROVISIONING.toString();

				int retries = 10;
				
				do {
					
					// sleep for 10 seconds before checking the vm status
					Utils.block(Utils.getApiRecheckDelay());

					String zoneId = config.getZoneId();
					zoneId = zoneId != null ? zoneId : this.zone;
					// check the instance status
					Instance instance = null;
					
					// seems the Api sometimes return a not found exception
					try {
						instance = this.getInstance(config.getInstanceId(), zoneId);
						status = instance.getStatus();
						log.info(config.getInstanceId() + ", current status is: " + status);
						
					} catch(GoogleJsonResponseException e) {
						if(e.getMessage().indexOf(NOT_FOUND) == 0) {
							log.warn("Instance not found: " + config.getInstanceId());
							if(retries <= 0) {
								throw e;
							}
							retries--;
							
						} else {
							throw e;
						}
					}
					// FIXME INFO: ecarf-evm-1422261030407, current status is: null
				} while (InstanceStatus.IN_PROGRESS.contains(status));

				if(InstanceStatus.TERMINATED.equals(status)) {
					success = false;
				}
			}
		}

		return success;
	}

	/**
	 * Delete the VMs provided in this config
	 * @param configs
	 * @throws IOException 
	 */
	@Override
	public void shutdownInstance(List<VMConfig> configs) throws IOException {
		for(VMConfig config: configs) {
			log.info("Deleting VM: " + config);
			String zoneId = config.getZoneId();
			zoneId = zoneId != null ? zoneId : this.zone;
			this.getCompute().instances().delete(this.projectId, zoneId, config.getInstanceId())
			.setOauthToken(this.getOAuthToken()).execute();
		}
	}

	/**
	 * Delete the currently running vm, i.e. self terminate
	 * @throws IOException 
	 */
	@Override
	public void shutdownInstance() throws IOException {

		log.info("Deleting VM: " + this.instanceId);
		this.getCompute().instances().delete(this.projectId, this.zone, this.instanceId)
		.setOauthToken(this.getOAuthToken()).execute();

	}

	/**
	 * Create an API Metadata
	 * @param vmMetaData
	 * @return
	 */
	private Metadata getMetaData(VMMetaData vmMetaData) {
		Metadata metadata = new Metadata();

		Items item;
		List<Items> items = new ArrayList<>();
		for(Entry<String, Object> entry: vmMetaData.getAttributes().entrySet()) {
			item = new Items();
			item.setKey(entry.getKey()).setValue((String) (entry.getValue()));
			items.add(item);
		}
		metadata.setItems(items);
		metadata.setFingerprint(vmMetaData.getFingerprint());

		return metadata;
	}


	//------------------------------------------------- Bigquery -------------------------------

	/**
	 * Runs a synchronous BigQuery query and displays the result.
	 * 
	 * CONFIG: {
		 "kind": "bigquery#queryResponse",
		 "schema": {
		  "fields": [
		   {
		    "name": "subject",
		    "type": "STRING",
		    "mode": "NULLABLE"
		   }
		  ]
		 },
		 "jobReference": {
		  "projectId": "ecarf-1000",
		  "jobId": "job_Kz8FdIf1RRYpfl7S5At069SmU0g"
		 },
		 "totalRows": "0",
		 "pageToken": "BFY5ITDWIUAQAAASAYIP777774DRUBQIAAIKBDIG",
		 "totalBytesProcessed": "0",
		 "jobComplete": true,
		 "cacheHit": true
		}
	 * 
	 * CONFIG: {
		 "kind": "bigquery#getQueryResultsResponse",
		 "etag": "\"QPJfVWBscaHhAhSLq0k5xRS6X5c/785hb4MiI7-vQ4YmGDJbd6kVc9o\"",
		 "schema": {
		  "fields": [
		   {
		    "name": "subject",
		    "type": "STRING",
		    "mode": "NULLABLE"
		   }
		  ]
		 },
		 "jobReference": {
		  "projectId": "ecarf-1000",
		  "jobId": "job_4vf8_9asbBIgspFLsEtr71BBMVk"
		 },
		 "totalRows": "0",
		 "pageToken": "CIDBB777777QOGQGBAABBIENAY======",
		 "jobComplete": true,
		 "cacheHit": true
		}
	 *
	 * @param query A String containing a BigQuery SQL statement
	 * @param out A PrintStream for output, normally System.out
	 */
	public void runBigDataQuery(String query, PrintStream out) throws IOException {
		QueryRequest queryRequest = new QueryRequest().setQuery(query);

		QueryResponse queryResponse = this.getBigquery().jobs()
				.query(this.projectId, queryRequest)
				.setOauthToken(this.getOAuthToken()).execute();

		if (queryResponse.getJobComplete()) {
			printRows(queryResponse.getRows(), out);
			if ((null == queryResponse.getPageToken()) || BigInteger.ZERO.equals(queryResponse.getTotalRows())) {
				return;
			}
		}
		// This loop polls until results are present, then loops over result pages.
		String pageToken = null;
		while (true) {
			GetQueryResultsResponse queryResults = this.getQueryResults(queryResponse.getJobReference().getJobId(), pageToken);

			if (queryResults.getJobComplete()) {

				printRows(queryResults.getRows(), out);
				pageToken = queryResults.getPageToken();
				if ((null == pageToken) || BigInteger.ZERO.equals(queryResults.getTotalRows())) {
					return;
				}
			}
		}
	}


	private static void printRows(List<TableRow> rows, PrintStream out) {
		if (rows != null) {
			for (TableRow row : rows) {
				for (TableCell cell : row.getF()) {
					// Data.isNull() is the recommended way to check for the 'null object' in TableCell.
					out.printf("%s, ", Data.isNull(cell.getV()) ? "null" : cell.getV().toString());
				}
				out.println();
			}
		}
	}

	/**
	 * CONFIG: {
		 "kind": "bigquery#job",
		 "etag": "\"QPJfVWBscaHhAhSLq0k5xRS6X5c/xAdd09GSpMDr9PxAk-WGEBWxlKA\"",
		 "id": "ecarf-1000:job_uUL5E0xmOjKxf3hREEZvb5B_M78",
		 "selfLink": "https://www.googleapis.com/bigquery/v2/projects/ecarf-1000/jobs/job_uUL5E0xmOjKxf3hREEZvb5B_M78",
		 "jobReference": {
		  "projectId": "ecarf-1000",
		  "jobId": "job_uUL5E0xmOjKxf3hREEZvb5B_M78"
		 },
		 "configuration": {
		  "load": {
		   "sourceUris": [
		    "gs://ecarf/umbel_links.nt_out.gz",
		    "gs://ecarf/yago_links.nt_out.gz"
		   ],
		   "schema": {
		    "fields": [
		     {
		      "name": "subject",
		      "type": "STRING"
		     },
		     {
		      "name": "object",
		      "type": "STRING"
		     },
		     {
		      "name": "predicate",
		      "type": "STRING"
		     }
		    ]
		   },
		   "destinationTable": {
		    "projectId": "ecarf-1000",
		    "datasetId": "swetodlp",
		    "tableId": "test"
		   }
		  }
		 },
		 "status": {
		  "state": "DONE"
		 },
		 "statistics": {
		  "creationTime": "1398091486326",
		  "startTime": "1398091498083",
		  "endTime": "1398091576483",
		  "load": {
		   "inputFiles": "2",
		   "inputFileBytes": "41510712",
		   "outputRows": "3782729",
		   "outputBytes": "554874551"
		  }
		 }
		}
	 * @param files - The source URIs must be fully-qualified, in the format gs://<bucket>/<object>.
	 * @param table
	 * @return
	 * @throws IOException
	 */
	@Override
	public String loadCloudStorageFilesIntoBigData(List<String> files, String table, boolean createTable) throws IOException {
		log.info("Loading data from files: " + files + ", into big data table: " + table);

		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationLoad load = new JobConfigurationLoad();	
		config.setLoad(load);
		job.setConfiguration(config);

		load.setSourceUris(files);
		load.setCreateDisposition(createTable ? CREATE_IF_NEEDED : CREATE_NEVER);
        load.setWriteDisposition(WRITE_APPEND);
		load.setSchema(GoogleMetaData.SCHEMA);

		String [] names = StringUtils.split(table, '.');
		TableReference tableRef = (new TableReference())
				.setProjectId(this.projectId)
				.setDatasetId(names[0])
				.setTableId(names[1]);
		load.setDestinationTable(tableRef);

		Bigquery.Jobs.Insert insert = this.getBigquery().jobs().insert(projectId, job);

		insert.setProjectId(projectId);
		insert.setOauthToken(this.getOAuthToken());

		JobReference jobRef = insert.execute().getJobReference();

		log.info("Job ID of Load Job is: " + jobRef.getJobId());

		// TODO add retry support
		return this.checkBigQueryJobResults(jobRef.getJobId(), false, true);

	}
	
	
	/**
	 * CONFIG: {
	 "kind": "bigquery#job",
	 "etag": "\"QPJfVWBscaHhAhSLq0k5xRS6X5c/RenEm3VqmGyNz-qo48hIw9I6GYQ\"",
	 "id": "ecarf-1000:job_gJed2_eIOXJaMi8RXKjps0hgFhY",
	 "selfLink": "https://www.googleapis.com/bigquery/v2/projects/ecarf-1000/jobs/job_gJed2_eIOXJaMi8RXKjps0hgFhY",
	 "jobReference": {
	  "projectId": "ecarf-1000",
	  "jobId": "job_gJed2_eIOXJaMi8RXKjps0hgFhY"
	 },
	 "configuration": {
	  "load": {
	   "schema": {
	    "fields": [
	     {
	      "name": "subject",
	      "type": "STRING"
	     },
	     {
	      "name": "object",
	      "type": "STRING"
	     },
	     {
	      "name": "predicate",
	      "type": "STRING"
	     }
	    ]
	   },
	   "destinationTable": {
	    "projectId": "ecarf-1000",
	    "datasetId": "swetodlp",
	    "tableId": "test"
	   },
	   "createDisposition": "CREATE_NEVER",
	   "encoding": "UTF-8"
	  }
	 },
	 "status": {
	  "state": "RUNNING"
	 },
	 "statistics": {
	  "creationTime": "1398092776236",
	  "startTime": "1398092822962",
	  "load": {
	   "inputFiles": "1",
	   "inputFileBytes": "8474390"
	  }
	 }
	}
	 * @param files
	 * @param table
	 * @param createTable
	 * @return
	 * @throws IOException
	 */
	@Override
	public List<String> loadLocalFilesIntoBigData(List<String> files, String table, boolean createTable) throws IOException {
		/*TableSchema schema = new TableSchema();
        schema.setFields(new ArrayList<TableFieldSchema>());
        JacksonFactory JACKSON = new JacksonFactory();
        JACKSON.createJsonParser(new FileInputStream("schema.json"))
        .parseArrayAndClose(schema.getFields(), TableFieldSchema.class, null);
        schema.setFactory(JACKSON);*/
		
		Stopwatch stopwatch = new Stopwatch();
		stopwatch.start();

        String [] names = StringUtils.split(table, '.');
		TableReference tableRef = (new TableReference())
				.setProjectId(this.projectId)
				.setDatasetId(names[0])
				.setTableId(names[1]);

        Job job = new Job();
        JobConfiguration config = new JobConfiguration();
        JobConfigurationLoad load = new JobConfigurationLoad();

        load.setSchema(GoogleMetaData.SCHEMA);
        load.setDestinationTable(tableRef);

        load.setEncoding(Constants.UTF8);
        load.setCreateDisposition(createTable ? CREATE_IF_NEEDED : CREATE_NEVER);
        load.setWriteDisposition(WRITE_APPEND);

        config.setLoad(load);
        job.setConfiguration(config);
        
        List<String> jobIds = new ArrayList<>();
        
        for(String file: files) {
        	FileContent content = new FileContent(Constants.BINARY_CONTENT_TYPE, new File(file));

        	Bigquery.Jobs.Insert insert = this.getBigquery().jobs().insert(projectId, job, content);

        	insert.setProjectId(projectId);
        	insert.setOauthToken(this.getOAuthToken());

        	JobReference jobRef = insert.execute().getJobReference();
        	jobIds.add(jobRef.getJobId());
        
        }
        List<String> completedIds = new ArrayList<>();
        
        for(String jobId: jobIds) {
        	// TODO add retry support
        	completedIds.add(this.checkBigQueryJobResults(jobId, false, false));
        }
        
        log.info("Uploaded " + files.size() + " files into bigquery in " + stopwatch );
        
		return completedIds;
		
	}

	/**
	 * Stream local N triple files into big query
	 * @param files
	 * @param table
	 * @throws IOException
	 */
	@Override
	public void streamLocalFilesIntoBigData(List<String> files, String table) throws IOException {
		Set<Triple> triples = null;
		for(String file: files) {
			triples = TripleUtils.loadNTriples(file);
			
			if(!triples.isEmpty()) {
				this.streamTriplesIntoBigData(triples, table);
			}
		}
	}
	
	/**
	 * Stream triple data into big query
	 * @param files
	 * @param table
	 * @param createTable
	 * @throws IOException
	 */
	@Override
	public void streamTriplesIntoBigData(Collection<Triple> triples, String table) throws IOException {
		
		Stopwatch stopwatch = new Stopwatch();
		stopwatch.start();

        String [] names = StringUtils.split(table, '.');

		String datasetId = names[0];
		String tableId = names[1];
		//String timestamp = Long.toString((new Date()).getTime());
		
		List<TableDataInsertAllRequest.Rows>  rowList = new ArrayList<>();
		
		//TableRow row;
		TableDataInsertAllRequest.Rows rows;

		for(Triple triple: triples) {
			//row = new TableRow();
			//row.set
			rows = new TableDataInsertAllRequest.Rows();
			//rows.setInsertId(timestamp);
			rows.setJson(triple.toMap());
			rowList.add(rows);
		}
		
		int maxBigQueryRequestSize = Config.getIntegerProperty("ecarf.io.google.bigquery.max.rows.per.request", 7000);
		int delay = Config.getIntegerProperty("ecarf.io.google.bigquery.stream.delay.seconds", 2);
		
		if(rowList.size() > maxBigQueryRequestSize) {
			
			int itr = (int) Math.ceil(rowList.size() * 1.0 / maxBigQueryRequestSize);
			
			for(int i = 1; i <= itr; i++) {
				
				int index;
				
				if(i == itr) {
					// last iteration
					index = rowList.size();
					
				} else {
					index = maxBigQueryRequestSize;
				}
				
				List<TableDataInsertAllRequest.Rows> requestRows = rowList.subList(0, index);
				this.streamRowsIntoBigQuery(datasetId, tableId, requestRows, 0);
				requestRows.clear();
				
				//block for a short moment to avoid rateLimitExceeded errors
				Utils.block(delay);
				
			}
			
		} else {
			this.streamRowsIntoBigQuery(datasetId, tableId, rowList, 0);
		}
		
		stopwatch.stop();
		
		log.info("Streamed " + triples.size() + " triples into bigquery in " + stopwatch );
		
	}
	
	/**
	 * Stream a list of rows into bigquery. Retries 3 times if the insert of some rows has failed, i.e. Bigquery returns
	 * an insert error
	 * 
	 *  {
		  "insertErrors" : [ {
		    "errors" : [ {
		      "reason" : "timeout"
		    } ],
		    "index" : 8
		  }],
  		  "kind" : "bigquery#tableDataInsertAllResponse"
  		}
	 *	  
	 * @param datasetId
	 * @param tableId
	 * @param rowList
	 * @throws IOException
	 */
	private void streamRowsIntoBigQuery(String datasetId, String tableId, 
			List<TableDataInsertAllRequest.Rows>  rowList, int retries) throws IOException {
		
		/*
		 * ExponentialBackOff backoff = ExponentialBackOff.builder()
	    .setInitialIntervalMillis(500)
	    .setMaxElapsedTimeMillis(900000)
	    .setMaxIntervalMillis(6000)
	    .setMultiplier(1.5)
	    .setRandomizationFactor(0.5)
	    .build();
		 */
		
		TableDataInsertAllRequest content = new TableDataInsertAllRequest().setRows(rowList);
		
		boolean retrying = false;
		ExponentialBackOff backOff = new ExponentialBackOff();
		
		TableDataInsertAllResponse response = null;
		// keep trying and exponentially backoff as needed
		do {
			try {

				response = this.getBigquery().tabledata().insertAll(
						this.projectId, datasetId, tableId, content)
						.setOauthToken(this.getOAuthToken()).execute();

				log.info(response.toPrettyString());
				retrying = false;

			} catch(GoogleJsonResponseException e) {
				

				GoogleJsonError error = e.getDetails();	
				
				// check for rate limit errors
				if((error != null) && (error.getErrors() != null) && !error.getErrors().isEmpty() &&
						GoogleMetaData.RATE_LIMIT_EXCEEDED.equals(error.getErrors().get(0).getReason())) {
					
					log.error("Failed to stream data, error: " + error.getMessage());
					
					long backOffTime = backOff.nextBackOffMillis();
					
					if (backOffTime == BackOff.STOP) {
						// we are not retrying anymore
						log.warn("Failed after " + backOff.getElapsedTimeMillis() / 1000 + " seconds of elapsed time");
						throw e;
						
					} else {
						int period = (int) Math.ceil(backOffTime / 1000);
						if(period == 0) {
							period = 1;
						}
						log.info("Backing off for " + period + " seconds.");
						Utils.block(period);
						retrying = true;
					}
					
				} else {
					log.error("Failed to stream data", e);
					throw e;
				}
			}
			
		} while(retrying);
		
		// check for failed rows
		if((response != null) && (response.getInsertErrors() != null) && !response.getInsertErrors().isEmpty()) {
			List<TableDataInsertAllRequest.Rows> failedList = new ArrayList<>();
			
			List<InsertErrors> insertErrors = response.getInsertErrors();
			
			for(InsertErrors error: insertErrors) {
				failedList.add(rowList.get(error.getIndex().intValue()));
			}
			
			// retry again for the failed list
			if(retries > Config.getIntegerProperty("ecarf.io.google.bigquery.insert.errors.retries", 3)) {
				
				log.warn("Failed to stream some rows into bigquery after 3 retries");
				throw new IOException("Failed to stream some rows into bigquery after 3 retries");
				
			} else {
				retries++;
				log.warn(failedList.size() + " rows failed to be inserted retrying again. Retries = " + retries); 
				this.streamRowsIntoBigQuery(datasetId, tableId, failedList, retries);
			}
		}
	}

	/**
	 * Creates an asynchronous Query Job for a particular query on a dataset
	 *
	 * @param bigquery  an authorized BigQuery client
	 * @param projectId a String containing the project ID
	 * @param querySql  the actual query string
	 * @return a reference to the inserted query job
	 * @throws IOException
	 */
	@Override
	public String startBigDataQuery(String querySql) throws IOException {

		log.info("Inserting Query Job: " + querySql);

		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		config.setQuery(queryConfig);

		job.setConfiguration(config);
		queryConfig.setQuery(querySql);

		com.google.api.services.bigquery.Bigquery.Jobs.Insert insert = 
				this.getBigquery().jobs().insert(projectId, job);

		insert.setProjectId(projectId);
		insert.setOauthToken(this.getOAuthToken());
		// TODO java.net.SocketTimeoutException: Read timed out
		JobReference jobRef = insert.execute().getJobReference();

		log.info("Job ID of Query Job is: " + jobRef.getJobId());

		return jobRef.getJobId();
	}

	/**
	 * Polls the status of a BigQuery job, returns Job reference if "Done"
	 * This method will block until the job status is Done
	 * @param jobId     a reference to an inserted query Job
	 * @return a reference to the completed Job
	 * @throws IOException
	 */
	protected String checkBigQueryJobResults(String jobId, boolean retry, boolean throwError) throws IOException {
		// Variables to keep track of total query time
		Stopwatch stopwatch = new Stopwatch();
		stopwatch.start();

		String status = null;
		Job pollJob = null;

		do {
			pollJob = this.getBigquery().jobs().get(projectId, jobId)
					.setOauthToken(this.getOAuthToken()).execute();

			status = pollJob.getStatus().getState();

			log.info("Job Status: " + status + ", elapsed time (secs): " + stopwatch);

			// Pause execution for one second before polling job status again, to
			// reduce unnecessary calls to the BigQUery API and lower overall
			// application bandwidth.
			if (!GoogleMetaData.DONE.equals(status)) {
				Utils.block(Utils.getApiRecheckDelay());
			}
			// TODO Error handling

		} while (!GoogleMetaData.DONE.equals(status));

		stopwatch.stop();
		
		String completedJobId = pollJob.getJobReference().getJobId();
		
		log.info("Job completed successfully" + pollJob.toPrettyString());
		this.printJobStats(pollJob);
		
		if(retry && (pollJob.getStatus().getErrorResult() != null)) {
			completedJobId = this.retryFailedBigQueryJob(pollJob);
		}
		
		if(throwError && (pollJob.getStatus().getErrorResult() != null)) {
			ErrorProto error = pollJob.getStatus().getErrorResult();
			log.info("Error result" + error);
			throw new IOException("message: " + error.getMessage() + ", reason: " + error.getReason());
		}

		return completedJobId;
	}
	
	/**
	 * Check a number of completed jobs
	 * @param jobId
	 * @return
	 * @throws IOException
	 */
	protected Job getCompletedBigQueryJob(String jobId, boolean prettyPrint) throws IOException {
		
		Job pollJob = this.getBigquery().jobs().get(projectId, jobId)
					.setOauthToken(this.getOAuthToken()).execute();
		
		String status = pollJob.getStatus().getState();

		if (!GoogleMetaData.DONE.equals(status)) {
			pollJob = null;
			log.warn("Job has not completed yet, skipping. JobId: " + jobId);
			
		} else {
			if(prettyPrint) {
				log.info("Job completed successfully" + pollJob.toPrettyString());
			}
			this.printJobStats(pollJob);
		}
		
		return pollJob;
	}
	
	/**
	 * Retry if a bigquery job has failed due to transient errors
	 * {
		  "configuration" : {
		    "query" : {
		      "createDisposition" : "CREATE_IF_NEEDED",
		      "destinationTable" : {
		        "datasetId" : "_f14a24df5a43859914cb508177aa01d64466d055",
		        "projectId" : "ecarf-1000",
		        "tableId" : "anon3fe271d7c2fafca6fbe9e0490a1488c103a3a8fd"
		      },
		      "query" : "select subject,object from [ontologies.swetodblp@-221459-] where predicate=\"<http://lsdis.cs.uga.edu/projects/semdis/opus#last_modified_date>\";",
		      "writeDisposition" : "WRITE_TRUNCATE"
		    }
		  },
		  "etag" : "\"lJkBaCYfTrFXwh5N7-r9owDp5yw/O-f9DO_VlENTr60IoQaXlNb3dFQ\"",
		  "id" : "ecarf-1000:job_QGAraWZPcZLYbm6IhmIyT7aOhmQ",
		  "jobReference" : {
		    "jobId" : "job_QGAraWZPcZLYbm6IhmIyT7aOhmQ",
		    "projectId" : "ecarf-1000"
		  },
		  "kind" : "bigquery#job",
		  "selfLink" : "https://www.googleapis.com/bigquery/v2/projects/ecarf-1000/jobs/job_QGAraWZPcZLYbm6IhmIyT7aOhmQ",
		  "statistics" : {
		    "creationTime" : "1399203726826",
		    "endTime" : "1399203727264",
		    "startTime" : "1399203727120"
		  },
		  "status" : {
		    "errorResult" : {
		      "message" : "Connection error. Please try again.",
		      "reason" : "backendError"
		    },
		    "errors" : [ {
		      "message" : "Connection error. Please try again.",
		      "reason" : "backendError"
		    } ],
		    "state" : "DONE"
		  }
		}
	 * @throws IOException 
	 */
	private String retryFailedBigQueryJob(Job job) throws IOException {
		String jobId = job.getJobReference().getJobId();
		log.info("Retrying failed job: " + jobId);
		log.info("Error result" + job.getStatus().getErrorResult());
		JobConfiguration config = job.getConfiguration();
		String newCompletedJobId = null;
		if((config != null) && (config.getQuery() != null)) {
			// get the query
			String query = config.getQuery().getQuery();
			// re-execute the query
			Utils.block(Utils.getApiRecheckDelay());
			String newJobId = startBigDataQuery(query);
			Utils.block(Utils.getApiRecheckDelay());
			newCompletedJobId = checkBigQueryJobResults(newJobId, false, false);
		}
		return newCompletedJobId;
	}
	
	/**
	 * Print the stats of a big query job
	 * @param job
	 */
	private void printJobStats(Job job) {
		try {
			JobStatistics stats = job.getStatistics();
			JobConfiguration config = job.getConfiguration();
			// log the query
			if(config != null) {
				log.info("query: " + (config.getQuery() != null ? config.getQuery().getQuery() : ""));
			}
			// log the total bytes processed
			JobStatistics2 qStats = stats.getQuery();
			if(qStats != null) {
				log.info("Total Bytes processed: " + ((double) qStats.getTotalBytesProcessed() / FileUtils.ONE_GB) + " GB");
				log.info("Cache hit: " + qStats.getCacheHit());
			}
			
			JobStatistics3 lStats = stats.getLoad();
			if(lStats != null) {
				log.info("Output rows: " + lStats.getOutputRows());
				
			}
			
			long time = stats.getEndTime() - stats.getCreationTime();
			log.info("Elapsed query time (ms): " + time);
			log.info("Elapsed query time (s): " + TimeUnit.MILLISECONDS.toSeconds(time));
			
		} catch(Exception e) {
			log.warn("failed to log job stats", e);
		}
	}

	/**
	 * Get a page of Bigquery rows
	 * CONFIG: {
		 "kind": "bigquery#job",
		 "etag": "\"QPJfVWBscaHhAhSLq0k5xRS6X5c/eSppPGGASS7YbZBbC4v1q6lTcGM\"",
		 "id": "ecarf-1000:job_CaN3ROCFJdK30hBl7GBMmnvspgc",
		 "selfLink": "https://www.googleapis.com/bigquery/v2/projects/ecarf-1000/jobs/job_CaN3ROCFJdK30hBl7GBMmnvspgc",
		 "jobReference": {
		  "projectId": "ecarf-1000",
		  "jobId": "job_CaN3ROCFJdK30hBl7GBMmnvspgc"
		 },
		 "configuration": {
		  "query": {
		   "query": "select subject from swetodblp.swetodblp_triple where object = \"\u003chttp://lsdis.cs.uga.edu/projects/semdis/opus#Article_in_Proceedings1\u003e\";",
		   "destinationTable": {
		    "projectId": "ecarf-1000",
		    "datasetId": "_f14a24df5a43859914cb508177aa01d64466d055",
		    "tableId": "anonc6f8ec7bfe8bbd6bd76bbac6ad8db54482cd8209"
		   },
		   "createDisposition": "CREATE_IF_NEEDED",
		   "writeDisposition": "WRITE_TRUNCATE"
		  }
		 },
		 "status": {
		  "state": "DONE"
		 },
		 "statistics": {
		  "creationTime": "1398030237040",
		  "startTime": "1398030237657",
		  "endTime": "1398030237801",
		  "totalBytesProcessed": "0",
		  "query": {
		   "totalBytesProcessed": "0",
		   "cacheHit": true
		  }
		 }
		}
	 * @return
	 * @throws IOException 
	 */
	protected GetQueryResultsResponse getQueryResults(String jobId, String pageToken) throws IOException {
		int retries = 3;
		boolean retrying = false;
		GetQueryResultsResponse queryResults = null;
		do {
			try {
				queryResults = this.getBigquery().jobs()
						.getQueryResults(projectId, jobId)
						.setPageToken(pageToken)
						.setOauthToken(this.getOAuthToken())
						.execute();
				 
				retrying = false;
						 
			} catch(IOException e) {
				log.error("failed to query job", e);
				retries--;
				if(retries == 0) {
					throw e;
				}
				log.info("Retrying again in 2 seconds");
				Utils.block(2);
				retrying = true;
			}

		} while(retrying);

		return queryResults;
	}

	/**
	 * Makes an API call to the BigQuery API
	 * @param completedJob to the completed Job
	 * @throws IOException
	 */
	protected void displayQueryResults(String jobId) throws IOException {

		String pageToken = null;
		BigInteger totalRows = null;

		do {

			GetQueryResultsResponse queryResult = this.getQueryResults(jobId, pageToken);

			pageToken = queryResult.getPageToken();
			totalRows = queryResult.getTotalRows();

			List<TableRow> rows = queryResult.getRows();

			if(rows != null) {
				System.out.print("\nQuery Results:\n------------\n");
				for (TableRow row : rows) {
					for (TableCell field : row.getF()) {
						System.out.printf("%-50s", field.getV());
					}
					System.out.println();
				}
			}

		} while((pageToken != null) && !BigInteger.ZERO.equals(totalRows));
	}


	/**
	 * Polls a big data job and once done save the results to a file
	 * @param jobId
	 * @param filename
	 * @throws IOException
	 */
	@Override
	public BigInteger saveBigQueryResultsToFile(String jobId, String filename) throws IOException {
		// query with retry support
		String completedJob = checkBigQueryJobResults(jobId, true, false);
		Joiner joiner = Joiner.on(',');
		String pageToken = null;
		BigInteger totalRows = null;
		Integer numFields = null;

		try(PrintWriter writer = new PrintWriter(new FileOutputStream(filename))) {

			do {

				GetQueryResultsResponse queryResult = this.getQueryResults(completedJob, pageToken);

				pageToken = queryResult.getPageToken();
				log.info("Page token: " + pageToken);

				if(totalRows == null) {
					totalRows = queryResult.getTotalRows();
					numFields = queryResult.getSchema().getFields().size();
					log.info("Total rows for query: " + totalRows);
				}

				List<TableRow> rows = queryResult.getRows();

				if(rows != null) {
					log.info("Saving " + rows.size() + ", records to file: " + filename);

					// save as CSV and properly escape the data to avoid failures on parsing
					// one field only
					if(numFields == 1) {
						for (TableRow row : rows) {
							writer.println(StringEscapeUtils.escapeCsv((String) row.getF().get(0).getV()));		
						}

					} else {
						// multiple fields
						for (TableRow row : rows) {
							List<Object> fields = new ArrayList<>();
							for (TableCell field : row.getF()) {
								fields.add(StringEscapeUtils.escapeCsv((String) field.getV()));
							}
							writer.println(joiner.join(fields));		
						}
					}
				}

			} while((pageToken != null) && !BigInteger.ZERO.equals(totalRows));
		}
		return totalRows;
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
				append("tokenExpire", this.tokenExpire).
				toString();
	}


	/**
	 * @param accessToken the accessToken to set
	 */
	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}


	/**
	 * @param projectId the projectId to set
	 */
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}


	/**
	 * @param tokenExpire the tokenExpire to set
	 */
	public void setTokenExpire(Date tokenExpire) {
		this.tokenExpire = tokenExpire;
	}


	/**
	 * @param zone the zone to set
	 */
	public void setZone(String zone) {
		this.zone = zone;
	}


	/**
	 * @param serviceAccount the serviceAccount to set
	 */
	public void setServiceAccount(String serviceAccount) {
		this.serviceAccount = serviceAccount;
	}


	/**
	 * @param scopes the scopes to set
	 */
	public void setScopes(List<String> scopes) {
		this.scopes = scopes;
	}


	/**
	 * @param instanceId the instanceId to set
	 */
	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}


	/**
	 * @return the instanceId
	 */
	@Override
	public String getInstanceId() {
		return instanceId;
	}


}
