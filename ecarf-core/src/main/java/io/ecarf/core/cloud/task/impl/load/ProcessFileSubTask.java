package io.ecarf.core.cloud.task.impl.load;

import io.ecarf.core.cloud.CloudService;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ProcessFileSubTask implements Callable<TermCounter> {
	
	private final static Log log = LogFactory.getLog(ProcessFileSubTask.class);

	private String file;
	private CloudService cloud;
	private TermCounter counter;
	private String bucket;

	public ProcessFileSubTask(String file, String bucket, TermCounter counter, CloudService cloud) {
		super();
		this.file = file;
		this.cloud = cloud;
		this.counter = counter;
		this.bucket = bucket;

	}

	@Override
	public TermCounter call() throws IOException {

		String localFile = Utils.TEMP_FOLDER + file;

		log.info("Downloading file: " + file);

		this.cloud.downloadObjectFromCloudStorage(file, localFile, bucket);

		// all downloaded, carryon now, process the files

		log.info("Processing file: " + localFile);
		String outFile = this.cloud.prepareForCloudDatabaseImport(localFile, counter);

		// once the processing is done then delete the local file
		Utils.deleteFile(localFile);

		// now upload the files again

		log.info("Uploading file: " + outFile);
		this.cloud.uploadFileToCloudStorage(outFile, bucket);

		// now delete all the locally processed files
		Utils.deleteFile(outFile);

		return counter;
	}

}