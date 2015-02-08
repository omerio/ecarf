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

import io.ecarf.core.cloud.VMMetaData;
import io.ecarf.core.cloud.types.VMStatus;
import io.ecarf.core.exceptions.NodeException;
import io.ecarf.core.partition.Item;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.io.Files;
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
	
	private static final int BUFFER = (int) FileUtils.ONE_KB * 8;
	
	//public static final CSVParser CSV_PARSER1 = new CSVParser();
	
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
	 * Convert a json string to map
	 * @param json
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> jsonToMap(String json) {
		return GSON.fromJson(json, HashMap.class);
	}
	
	/**
	 * comma separated string to set
	 * @param csv
	 * @return
	 */
	public static Set<String> csvToSet(String csv) {
		Set<String> tokens = new HashSet<>();
		if(StringUtils.isNotBlank(csv)) {	
			String [] tokensArr = StringUtils.split(csv, ',');
			// clean up the tokens
			//tokens = Sets.newHashSet(tokensArr);
			for(String token: tokensArr) {
				tokens.add(token.trim());
			}
		} 
		return tokens; 
	}
	
	/**
	 * retrive the value of a json property
	 * @param json
	 * @param key
	 * @return
	 */
	public static String getStringPropertyFromJson(String json, String key) {
		Map<String, Object> map = jsonToMap(json);
		
		return (String) map.get(key);
	}
	
	/**
	 * Delete the file with the provided string
	 * @param filename
	 * @return
	 */
	public static boolean deleteFile(String filename) {
		File file = new File(filename);
		return file.delete();
	}
	
	/**
	 * copy the file with the provided string
	 * @param filename
	 * @return
	 * @throws IOException 
	 */
	public static void copyFile(String filename, String newName) throws IOException {
		File file = new File(filename);
		Files.copy(file, new File(newName));
	}
	
	/**
	 * Block for the number of provided seconds
	 * @param seconds
	 */
	public static void block(int seconds) {
		try {
			TimeUnit.SECONDS.sleep(seconds);
			//Thread.sleep(DateUtils.MILLIS_PER_SECOND * seconds);
		} catch (InterruptedException e1) {
			log.warn("wait interrupted", e1);
		}
	}
	
	/**
	 * Wait until two values become equal
	 * @param value1
	 * @param value2
	 */
	public static void waitForEquality(Object value1, Object value2, int seconds) {
		while(!value1.equals(value2)) {
			block(seconds);
		}
	}
		
	/**
	 * Convert a json file to set
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public static Set<String> jsonFileToSet(String filename) throws IOException {
		
		try(FileReader reader = new FileReader(filename)) {
			
			return GSON.fromJson(new JsonReader(reader),  
					new TypeToken<Set<String>>(){}.getType());
			
		} catch (Exception e) {
			log.error("failed to prase json into set", e);
			throw new IOException(e);
		}
	}
	
	/**
	 * Convert a json file to map
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public static Map<String, Long> jsonFileToMap(String filename) throws IOException {
		
		try(FileReader reader = new FileReader(filename)) {
			
			return GSON.fromJson(new JsonReader(reader),  
					new TypeToken<Map<String, Long>>(){}.getType());
			
		} catch (Exception e) {
			log.error("failed to prase json into map", e);
			throw new IOException(e);
		}
	}
	
	
	
	/**
	 * 
	 * @param filename
	 * @param object
	 * @throws IOException
	 */
	public static void objectToJsonFile(String filename, Object object) throws IOException {
		try(FileWriter writer = new FileWriter(filename)) {
			GSON.toJson(object, writer);
		}
	}
	
	/**
	 * Encode the provided text as filename
	 * @param text
	 * @return
	 */
	public static String encodeFilename(String text) {
		String filename = text;
		try {
			filename = URLEncoder.encode(text, Constants.UTF8);
		} catch (UnsupportedEncodingException e) {
			log.warn("Failed to encode text as filename: " + text, e);
		}
		return filename;
	}
	
	
	/**
	 * (Gzip) Uncompress a compressed file
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public String unCompressFile(String filename) throws IOException {
		FileInputStream fin = new FileInputStream(filename);
		BufferedInputStream in = new BufferedInputStream(fin);
		String outFile = GzipUtils.getUncompressedFilename(filename);
		try(FileOutputStream out = new FileOutputStream(outFile);
				GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in)) {
			final byte[] buffer = new byte[BUFFER];
			int n = 0;
			while (-1 != (n = gzIn.read(buffer))) {
				out.write(buffer, 0, n);
			}
		}
		return outFile;
	}
	
	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		Set<String> schemaTerms = Utils.GSON.fromJson(new JsonReader(new FileReader(Utils.TEMP_FOLDER + "test.json")),  
				new TypeToken<Set<String>>(){}.getType());
		System.out.println(schemaTerms);
	}
	
	/**
	 * Sums a list of numbers
	 * @param items
	 * @return
	 */
	public static Long sum(List<Item> items) {
		long sum = 0;
		for(Item item: items) {
			sum += item.getWeight();
		}
		return sum;
	}
	
	/**
	 * Sums a list of numbers, if a number is greater than max only max will be summed
	 * @param items
	 * @return
	 */
	public static Long sum(List<Item> items, long max) {
		long sum = 0;
		for(Item item: items) {
			Long weight = item.getWeight();
			if(weight > max) {
				weight = max;
			}
			sum += weight;
		}
		return sum;
	}
	
	/**
	 * Set the scale of items based on the max provided
	 * 1/2, 1, 2, 3, 4, 5, ..., n
	 * @param max
	 */
	public static List<Item> setScale(List<Item> items, long max) {
		for(Item item: items) {
			long weight = item.getWeight();
			Double scale = (double) weight / (double) max;
			if(scale >= 1d) {
				scale = (new BigDecimal(scale)).setScale(0, RoundingMode.HALF_UP).doubleValue();
			} else if(scale < 1d && scale > 0.5d) {
				scale = 1d;
			} else if(scale <= 0.5d) {
				scale = 0.5d;
			}
			
			item.setScale(scale);
		}
		return items;
	}
	
	/**
	 * Helper to get the api delay from the configurations
	 * @return
	 */
	public static int getApiRecheckDelay() {
		return Config.getIntegerProperty(Constants.API_DELAY_KEY, Constants.API_RECHECK_DELAY);
	}
	
	/**
	 * From an exception populate an ecarf metadata error
	 * @param metadata
	 * @param e
	 */
	public static void exceptionToEcarfError(VMMetaData metadata, Exception e) {
		metadata.addValue(VMMetaData.ECARF_STATUS, VMStatus.ERROR.toString());
		metadata.addValue(VMMetaData.ECARF_EXCEPTION, e.getClass().getName());
		metadata.addValue(VMMetaData.ECARF_MESSAGE, e.getMessage());
	}
	
	/**
	 * Return an IOException from the metaData error
	 * @param metaData
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static IOException exceptionFromEcarfError(VMMetaData metaData, String instanceId) {
		Class clazz = IOException.class;
		String message = metaData.getMessage();
		if(StringUtils.isNoneBlank(metaData.getException())) {
			try {
				clazz = Class.forName(metaData.getException());
			} catch (ClassNotFoundException e) {
				log.warn("failed to load exception class from evm");
			}
		}
		Exception cause;
		try {
			Constructor ctor = clazz.getDeclaredConstructor(String.class);
			ctor.setAccessible(true);
			cause = (Exception) ctor.newInstance(message);
		} catch (Exception e) {
			log.warn("failed to load exception class from evm");
			cause = new IOException(message);
		}
		return new NodeException(Constants.EVM_EXCEPTION + instanceId, cause, instanceId);
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
