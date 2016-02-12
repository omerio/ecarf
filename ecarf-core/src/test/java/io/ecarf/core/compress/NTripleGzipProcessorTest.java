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
package io.ecarf.core.compress;

import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudService;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.term.TermCounter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class NTripleGzipProcessorTest {
	
	private static final String IN_FILE = "/Users/omerio/Ontologies/dbpedia/revision_ids_en.nt.gz";
	private static final String OUT_FILE = "/Users/omerio/Ontologies/dbpedia/revision_ids_en.nt_out.gz";
	
	/**
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String [] args) throws IOException {
		
		try {
			Files.delete(Paths.get(OUT_FILE));
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		TermCounter counter = new TermCounter();
		counter.setTermsToCount(new HashSet<String>());
		
		EcarfGoogleCloudService service = new EcarfGoogleCloudServiceImpl();
		
		long startTime = System.nanoTime();
		String outFile = service.prepareForBigQueryImport(IN_FILE, counter, false);
		long estimatedTime = System.nanoTime() - startTime;
		
		double seconds = (double)estimatedTime / 1000000000.0;
		
		System.out.println("total time in seconds = " + seconds);
		System.out.println("Output file: " + outFile);
		
		
	}

}
