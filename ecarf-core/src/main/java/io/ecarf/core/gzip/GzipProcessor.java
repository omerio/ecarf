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
package io.ecarf.core.gzip;

import io.ecarf.core.utils.Constants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Processes a normal/gzip input file and outputs
 * to a gzip file
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class GzipProcessor {
	
	private String inputFile;
	
	private String outputFile;
	
	
	
	/**
	 * @param inputFile
	 * @param outputFile
	 */
	public GzipProcessor(String inputFile) {
		super();
		this.inputFile = inputFile;
		// get the file name before the ext
		String ext = FilenameUtils.getExtension(inputFile);
		// construct an output file in the format inputfile_out.ext
		this.outputFile = StringUtils.removeEnd(inputFile, "." + ext);	
		this.outputFile = outputFile + "_out." + ext;
	}

	/**
	 * Reads the input file, gunziped if needed, calls the callback to process
	 * each line that being read then writes the file back to a gziped output file
	 * @param callback
	 * @throws IOException 
	 */
	public String process(Callback callback) throws IOException {

		try(InputStream fileIn = new FileInputStream(this.inputFile);) {

			InputStream deflated = fileIn;
			if(GzipUtils.isCompressedFilename(this.inputFile)) {
				deflated = new GZIPInputStream(fileIn);
			} 

			try(BufferedReader bf = new BufferedReader(new InputStreamReader(deflated, Constants.UTF8));
					PrintWriter writer = new PrintWriter(new GZIPOutputStream(new FileOutputStream(this.outputFile)));) {
				String outLine;
				String inLine;
				while ((inLine = bf.readLine()) != null) {
					outLine = callback.process(inLine);
					if(outLine != null) {
						writer.println(outLine);
					}
				}
			}
			
			return this.outputFile;
		}
	}

}
