/*
 * Copyright (c) 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.ecarf.core.cloud.impl.google.samples;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeScopes;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;

/**
 * Main class for the Compute Engine API command line sample.
 * Demonstrates how to make an authenticated API call using OAuth 2 helper classes.
 */
public class ComputeSample {

	/**
	 * Be sure to specify the name of your application. If the application name is {@code null} or
	 * blank, the application will log a warning. Suggested format is "MyCompany-ProductName/1.0".
	 */
	private static final String APPLICATION_NAME = "";

	/** Set projectId to your Project ID from Overview pane in the APIs console */
	private static final String projectId = "YOUR_PROJECT_ID";

	/** Set Compute Engine zone */
	private static final String zoneName = "us-central1-a";

	/** Directory to store user credentials. */
	private static final java.io.File DATA_STORE_DIR =
			new java.io.File(System.getProperty("user.home"), ".store/compute_sample");

	/**
	 * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
	 * globally shared instance across your application.
	 */
	private static FileDataStoreFactory dataStoreFactory;

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	/** Global instance of the HTTP transport. */
	private static HttpTransport httpTransport;

	@SuppressWarnings("unused")
	private static Compute client;

	/** Authorizes the installed application to access user's protected data. */
	private static Credential authorize() throws Exception {
		// load client secrets
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
				new InputStreamReader(ComputeSample.class.getResourceAsStream("/client_secrets.json")));
		if (clientSecrets.getDetails().getClientId().startsWith("Enter") ||
				clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
			System.out.println(
					"Overwrite the src/main/resources/client_secrets.json file with the client secrets file "
							+ "you downloaded from the Quickstart tool or manually enter your Client ID and Secret "
							+ "from https://code.google.com/apis/console/?api=compute#project:315344313954 "
							+ "into src/main/resources/client_secrets.json");
			System.exit(1);
		}

		// Set up authorization code flow.
		// Ask for only the permissions you need. Asking for more permissions will
		// reduce the number of users who finish the process for giving you access
		// to their accounts. It will also increase the amount of effort you will
		// have to spend explaining to users what you are doing with their data.
		// Here we are listing all of the available scopes. You should remove scopes
		// that you are not actually using.
		Set<String> scopes = new HashSet<String>();
		scopes.add(ComputeScopes.COMPUTE);
		scopes.add(ComputeScopes.COMPUTE_READONLY);
		scopes.add(ComputeScopes.DEVSTORAGE_FULL_CONTROL);
		scopes.add(ComputeScopes.DEVSTORAGE_READ_ONLY);
		scopes.add(ComputeScopes.DEVSTORAGE_READ_WRITE);

		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				httpTransport, JSON_FACTORY, clientSecrets, scopes)
		.setDataStoreFactory(dataStoreFactory)
		.build();
		// authorize
		return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
	}

	public static void main(String[] args) {
		try {
			// initialize the transport
			httpTransport = GoogleNetHttpTransport.newTrustedTransport();

			// initialize the data store factory
			dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);

			// authorization
			Credential credential = authorize();

			// set up global Compute instance
			client = new Compute.Builder(httpTransport, JSON_FACTORY, credential)
			.setApplicationName(APPLICATION_NAME).build();

			// List out instances
			printInstances(client, projectId);

			System.out.println("Success! Now add code here.");

		} catch (IOException e) {
			System.err.println(e.getMessage());
		} catch (Throwable t) {
			t.printStackTrace();
		}
		System.exit(1);
	}



	/**
	 * Print available machine instances.
	 *
	 * @param compute The main API access point
	 * @param projectId The project ID.
	 */
	public static void printInstances(Compute compute, String projectId) throws IOException {
		System.out.println("================== Listing Compute Engine Instances ==================");
		Compute.Instances.List instances = compute.instances().list(projectId, zoneName);
		InstanceList list = instances.execute();
		if (list.getItems() == null) {
			System.out.println("No instances found. Sign in to the Google APIs Console and create "
					+ "an instance at: code.google.com/apis/console");
		} else {
			for (Instance instance : list.getItems()) {
				System.out.println(instance.toPrettyString());
			}
		}
	}
}

