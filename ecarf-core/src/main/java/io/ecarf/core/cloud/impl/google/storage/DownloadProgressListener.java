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

import java.util.logging.Logger;

import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.googleapis.media.MediaHttpDownloaderProgressListener;
import com.google.common.base.Stopwatch;

/**
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public  class DownloadProgressListener implements MediaHttpDownloaderProgressListener {
	
	private final static Logger log = Logger.getLogger(DownloadProgressListener.class.getName()); 
	
	private final Stopwatch stopwatch;
	
	private Callback callback;

	public DownloadProgressListener(Callback callback) {
		this.stopwatch = new Stopwatch();
		this.stopwatch.start();
		this.callback = callback;
	}

	@Override
	public void progressChanged(MediaHttpDownloader downloader) {
		switch (downloader.getDownloadState()) {
		case MEDIA_IN_PROGRESS:
			log.info(Double.toString(downloader.getProgress()));
			break;
		case MEDIA_COMPLETE:
			stopwatch.stop();
			log.info(String.format("Download is complete! (%s)", stopwatch));
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