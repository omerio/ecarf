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
package io.ecarf.core.cloud.task;

import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.compress.NTripleGzipProcessor;
import io.ecarf.core.compress.callback.ExtractTermsCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.term.TermDictionary;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.TestUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Stopwatch;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ProcessLoadTaskTest {

    private EcarfGoogleCloudServiceImpl service;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        this.service = new EcarfGoogleCloudServiceImpl();
        TestUtils.prepare(service);
    }


    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link io.ecarf.core.cloud.task.processor.old.ProcessLoadTask#run()}.
     * @throws IOException 
     */
    @Test
    @Ignore
    public void testRun() throws IOException {

        /*ProcessLoadTask task = new ProcessLoadTask();
        task.setCloudService(service);
        task.setBucket("ecarf");
        task.setFiles("redirects_transitive_en.nt.gz");
        task.setSchemaTermsFile("schema_terms.json");
        task.run();*/
    }

    /**
     * 
     * @param args
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        /*try(Scanner reader = new Scanner(System.in);) {
	        System.out.println("Attache profiler then presss any key to continue");
	        reader.next();
	    }
	    System.out.println("Processing Data");*/
        //System.out.println(GzipUtils.getUncompressedFilename("/blah/blah/myfile.json.gz"));
        Stopwatch stopwatch = Stopwatch.createStarted();
        String filename = "/Users/omerio/Ontologies/swetodblp_2008_8.nt.gz";
        String termsFile = "/Users/omerio/SkyDrive/PhD/Experiments/phase2/05_09_2015_SwetoDblp_2n/schema_terms.txt";

        Set<String> schemaTerms = FileUtils.jsonFileToSet(termsFile);
        TermCounter counter = new TermCounter();
        counter.setTermsToCount(schemaTerms);

        NTripleGzipProcessor processor = new NTripleGzipProcessor(filename);

        //NTripleGzipCallback callback = new CommonsCsvCallback();
        ExtractTermsCallback callback = new ExtractTermsCallback();

        callback.setCounter(counter);

        //String outFilename = processor.process(callback);
        processor.read(callback);

        //System.out.println("Created out file: " + outFilename + ", in: " + stopwatch);
        System.out.println("Processed file in: " + stopwatch);

        System.out.println("\n" + counter.getCount());

        System.out.println("Number of unique terms: " + callback.getResources().size());
        System.out.println("Number of blank nodes: " + callback.getBlankNodes().size());
        System.out.println("Number of literals: " + callback.getLiteralCount());

        Set<String> resources = callback.getResources();

        TermDictionary dictionary = new TermDictionary();

        for(String term: resources) {
            dictionary.add(term);
        }

        String term = "<http://dblp.uni-trier.de/rec/bibtex/journals/ijcsa/AndonoffBH07>";
        String dicFile1 = FileUtils.TEMP_FOLDER + Constants.DICTIONARY_SER;
        
        // --- json
        
        // ---  object serialization
        Stopwatch stopwatch1 = Stopwatch.createStarted();
        String compressedFile = dictionary.toFile(dicFile1, true);
        System.out.println("(Java) Dictionary file saved to: " + compressedFile + ", timer: " + stopwatch1);
        Integer id = dictionary.encode(term);
        System.out.println(id);
        
        stopwatch1.reset();
        stopwatch1.start();

        dictionary = TermDictionary.fromFile(compressedFile, true);
        System.out.println("(Java)Dictionary loaded from file, timer: " + stopwatch1);
        id = dictionary.encode(term);
        System.out.println(id);
        
        // ------ kryo serialization
        stopwatch1.reset();
        stopwatch1.start();
        dicFile1 = FileUtils.TEMP_FOLDER + "dictionary.kryo" + Constants.GZIP_EXT;
        
        Utils.objectToFile(dicFile1, dictionary, true, false);
        System.out.println("(Kryo) Dictionary file saved to: " + dicFile1 + ", timer: " + stopwatch1);
        id = dictionary.encode(term);
        System.out.println(id);
        
        stopwatch1.reset();
        stopwatch1.start();

        dictionary = Utils.objectFromFile(dicFile1, TermDictionary.class, true, false);
        System.out.println("(Kryo) Dictionary loaded from file, timer: " + stopwatch1);
        id = dictionary.encode(term);
        System.out.println(id);
        
        //System.out.println("Processed file and dictionary in: " + stopwatch);
        
        

    }

}
