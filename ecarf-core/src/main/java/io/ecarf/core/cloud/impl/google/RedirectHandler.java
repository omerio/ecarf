package io.ecarf.core.cloud.impl.google;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;

/**
 * 
 * Storage API request from Compute Engine service account doesn't append OAuth token on redirected URL
 * 
 * @see https://code.google.com/p/google-api-java-client/issues/detail?id=866
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class RedirectHandler implements HttpUnsuccessfulResponseHandler {
	
	private final static Log log = LogFactory.getLog(RedirectHandler.class);

	private static final String OAUTH_TOKEN_PARAM = "?oauth_token=";
	/*
	 * (non-Javadoc)
	 * @see com.google.api.client.http.HttpUnsuccessfulResponseHandler#handleResponse(
	 * com.google.api.client.http.HttpRequest, com.google.api.client.http.HttpResponse, boolean)
	 */
	public boolean handleResponse(
			HttpRequest request, HttpResponse response, boolean retrySupported) throws IOException {
		if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_TEMPORARY_REDIRECT) {

			String redirectLocation = response.getHeaders().getLocation();
			if (request.getFollowRedirects() && redirectLocation != null) {
				
				String url = request.getUrl().toString();
				String oauthToken = StringUtils.substringAfterLast(url, OAUTH_TOKEN_PARAM);
				// resolve the redirect location relative to the current location
				// re-append the oauth token request parameter
				request.setUrl(new GenericUrl(request.getUrl().toURL(redirectLocation + OAUTH_TOKEN_PARAM + oauthToken)));
				return true;
			}
		}
		return false;
	}
}