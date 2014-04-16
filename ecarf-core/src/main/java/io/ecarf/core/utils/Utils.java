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

import io.ecarf.core.partition.Item;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.io.FileUtils;

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
	
	private final static Logger log = Logger.getLogger(Utils.class.getName()); 

	public static final String TEMP_FOLDER = System.getProperty("java.io.tmpdir");
	
	public static final String PATH_SEPARATOR = File.separator;
	
	public static Gson GSON = new Gson();
	
	private static final int BUFFER = (int) FileUtils.ONE_KB * 8;
	

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
	 * Block for the number of provided seconds
	 * @param seconds
	 */
	public static void block(int seconds) {
		try {
			TimeUnit.SECONDS.sleep(seconds);
			//Thread.sleep(DateUtils.MILLIS_PER_SECOND * seconds);
		} catch (InterruptedException e1) {
			log.log(Level.WARNING, "wait interrupted", e1);
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
			log.log(Level.SEVERE, "failed to prase json into set", e);
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
			log.log(Level.SEVERE, "failed to prase json into map", e);
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
	
	public static int getApiRecheckDelay() {
		return Config.getIntegerProperty(Constants.API_DELAY_KEY, Constants.API_RECHECK_DELAY);
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

}
