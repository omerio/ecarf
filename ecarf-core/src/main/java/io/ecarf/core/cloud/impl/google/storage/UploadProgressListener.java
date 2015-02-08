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
package io.ecarf.core.cloud.impl.google.storage;

import io.ecarf.core.utils.Callback;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.common.base.Stopwatch;


/**
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class UploadProgressListener implements MediaHttpUploaderProgressListener {
	
	private final static Log log = LogFactory.getLog(UploadProgressListener.class);
	
	private final Stopwatch stopwatch = new Stopwatch();
	
	private Callback callback;

	/**
	 * @param callback
	 */
	public UploadProgressListener(Callback callback) {
		super();
		this.callback = callback;
	}

	@Override
	public void progressChanged(MediaHttpUploader uploader) {
		switch (uploader.getUploadState()) {
		case INITIATION_STARTED:
			stopwatch.start();
			log.info("Initiation has started!");
			break;
		case INITIATION_COMPLETE:
			log.info("Initiation is complete!");
			break;
		case MEDIA_IN_PROGRESS:
			// Progress works iff you have a content length specified.
			try {
				log.info("Progress: " + Math.round(uploader.getProgress() * 100.00) + "%");
			} catch (IOException e) {
				log.warn("Failed to get progress", e);
				log.info("Uploaded: " + uploader.getNumBytesUploaded());
			}
			
			break;
		case MEDIA_COMPLETE:
			if(stopwatch.isRunning()) {
				stopwatch.stop();
			}
			log.info(String.format("Upload is complete! (%s)", stopwatch));
			if(this.callback != null) {
				this.callback.execute();
			}
			break;
		case NOT_STARTED:
			break;
		}
	}


	/**
	 * @param callback the callback to set
	 */
	public void setCallback(Callback callback) {
		this.callback = callback;
	}
}