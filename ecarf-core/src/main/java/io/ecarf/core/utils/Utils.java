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

import io.cloudex.framework.cloud.api.ApiUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Utils {
	
	private final static Log log = LogFactory.getLog(Utils.class);

	public static final String PATH_SEPARATOR = File.separator;
	
	public static final String TEMP_FOLDER = System.getProperty("java.io.tmpdir") + PATH_SEPARATOR;
	
	public static final byte [] SEPARATOR = System.getProperty("line.separator").getBytes();
	
	public static Gson GSON = new Gson();

	/**
	 * Copy a url to a local file
	 * @param url
	 * @param file
	 * @throws IOException 
	 */
	public static void downloadFile(String url, String file) throws IOException {
		log.info("Downloading file from: " + url);
		URL webfile = new URL(url);	
		try(ReadableByteChannel rbc = Channels.newChannel(webfile.openStream());
				FileOutputStream fos = new FileOutputStream(file)) {
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		}
	}
	
	/**
	 * Wait until two values become equal
	 * @param value1
	 * @param value2
	 */
	public static void waitForEquality(Object value1, Object value2, int seconds) {
		while(!value1.equals(value2)) {
			ApiUtils.block(seconds);
		}
	}
	
	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		Set<String> schemaTerms = Utils.GSON.fromJson(new JsonReader(new FileReader(Utils.TEMP_FOLDER + "test.json")),  
				new TypeToken<Set<String>>(){}.getType());
		System.out.println(schemaTerms);
	}
	
	/**
	 * Create a fixed size thread pool
	 * @param numOfThreads, the number of threads, if null defaults to the number of 
	 * 			processors
	 * @return
	 */
	public static ExecutorService createFixedThreadPool(Integer numOfThreads) {
		if(numOfThreads == null) {
			numOfThreads = Runtime.getRuntime().availableProcessors();
		}
		log.info("Creating fixed thread pool of size: " + numOfThreads);
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		return executor;
	}
	
	public static int getStringSize(String text) {
		int size = 0;
		
		if(StringUtils.isNotBlank(text)) {
			try {
				size = text.getBytes(Constants.UTF8).length;
			} catch (UnsupportedEncodingException e) {
				log.warn("Failed to encode text as UTF8: " + text, e);
			}
		}
		
		return size;
	}
	
	
//	/*
//	 * Determines if a byte array is compressed. The java.util.zip GZip
//	 * Implementation does not expose the GZip header so it is difficult to determine
//	 * if a string is compressed.
//	 * 
//	 * @param bytes an array of bytes
//	 * @return true if the array is compressed or false otherwise
//	 * @throws java.io.IOException if the byte array couldn't be read
//	 */
//	public static boolean isGziped(byte[] bytes) throws IOException {
//		boolean compressed = false;
//		if ((bytes == null) || (bytes.length < 2)) {
//
//			compressed = ((bytes[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && 
//					(bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8)));
//		}
//
//		return compressed;
//	}
//
//	/**
//	 * Checks if an input stream is gzipped.
//	 * 
//	 * @param in
//	 * @return
//	 */
//	public static boolean isGZipped(InputStream in) {
//		if (!in.markSupported()) {
//			in = new BufferedInputStream(in);
//		}
//		in.mark(2);
//		int magic = 0;
//		try {
//			magic = in.read() & 0xff | ((in.read() << 8) & 0xff00);
//			in.reset();
//		} catch (IOException e) {
//			log.log(Level.SEVERE, "Failed to read file", e);
//			return false;
//		}
//		return magic == GZIPInputStream.GZIP_MAGIC;
//	}
//
//	/**
//	 * Checks if a file is gzipped.
//	 * 
//	 * @param f
//	 * @return
//	 */
//	public static boolean isGZipped(File f) {
//		int magic = 0;
//		try {
//			RandomAccessFile raf = new RandomAccessFile(f, "r");
//			magic = raf.read() & 0xff | ((raf.read() << 8) & 0xff00);
//			raf.close();
//		} catch (Throwable e) {
//			log.log(Level.SEVERE, "Failed to read file", e);
//		}
//		return magic == GZIPInputStream.GZIP_MAGIC;
//	}
	
	/**
	   * Converts the query results retrieved from the datastore to json parsable by javascript
	   * into a DataTable object for use with a motion chart.
	   */
	/*  private void writeResultsToMotionChartJson(JsonWriter jsonWriter, Iterable<Entity> results)
	      throws IOException {
	    jsonWriter.name("data").beginObject();

	    // Write the header.
	    jsonWriter.name("cols").beginArray();
	    for (int i = 0; i < properties.length; i++) {
	      jsonWriter.beginObject()
	          .name("id").value(properties[i])
	          .name("label").value(labels[i])
	          .name("type").value(types[i])
	          .endObject();
	    }
	    jsonWriter.endArray();

	    // Write the data.
	    jsonWriter.name("rows").beginArray();
	    for (Entity entity : results) {
	      jsonWriter.beginObject().name("c").beginArray();
	      for (int i = 0; i < properties.length; i++) {
	        String value = "";
	        if (entity.getProperty(properties[i]) != null) {
	          value = String.valueOf(entity.getProperty(properties[i]));
	        }

	        jsonWriter.beginObject().name("v").value(value).endObject();
	      }
	      jsonWriter.endArray().endObject();
	    }
	    jsonWriter.endArray();

	    jsonWriter.endObject();
	  }*/

}
