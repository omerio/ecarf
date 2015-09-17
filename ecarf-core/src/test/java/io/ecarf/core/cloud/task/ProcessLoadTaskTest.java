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
import io.ecarf.core.compress.NTripleGzipCallback;
import io.ecarf.core.compress.NTripleGzipProcessor;
import io.ecarf.core.compress.callback.ExtractTermsCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.term.TermDictionary;
import io.ecarf.core.term.TermPart;
import io.ecarf.core.term.TermRoot;
import io.ecarf.core.term.TermUtils;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.TestUtils;
import io.ecarf.core.utils.Utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

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
    
    private static void testExtratCommonURIs() throws IOException {
        //String filename = "/Users/omerio/Ontologies/swetodblp_2008_1.nt.gz";
        //String filename = "/Users/omerio/Ontologies/dbpedia/page_ids_en.nt.gz";
        String filename = "/Users/omerio/Ontologies/dbpedia/external_links_en.nt.gz";
        String termsFile = "/Users/omerio/SkyDrive/PhD/Experiments/phase2/05_09_2015_SwetoDblp_2n/schema_terms.txt";

        Set<String> schemaTerms = FileUtils.jsonFileToSet(termsFile);
        TermCounter counter = new TermCounter();
        counter.setTermsToCount(schemaTerms);
        
        //final Map<String, Integer> uris = new TreeMap<>();
        final Set<String> resources = new HashSet<>();
        final TermRoot root = new TermRoot();
        final Set<String> blankNodes = new HashSet<>();
        final Map<String, Object> results = new HashMap<>();
        results.put("literalCount", 0);
        results.put("resources", 0);
        results.put("maxParts", 0);
        //final Set<String> schemaURIs = new HashSet<>();
        
       // final Set<String> schemaURIParts = Sets.newHashSet("http://www.w3.org/1999/02", "http://www.w3.org/2000/01", "http://www.w3.org/2002/07", "http://www.w3.org/TR");

        NTripleGzipProcessor processor = new NTripleGzipProcessor(filename);
        NTripleGzipCallback callback = new NTripleGzipCallback() {

            private TermCounter counter;
            
            private int maxParts;

            @Override
            public void setOutput(Appendable out) throws IOException {                
            }

            @Override
            public String process(Node[] nodes) throws IOException {
                
                String term;

                for (int i = 0; i < nodes.length; i++)  {
                    
                    // we are not going to unscape literals, these can contain new line and 
                    // unscaping those will slow down the bigquery load, unless offcourse we use JSON
                    // instead of CSV https://cloud.google.com/bigquery/preparing-data-for-bigquery
                    if((i == 2) && (nodes[i] instanceof Literal)) {

                        Integer literalCount = (Integer) results.get("literalCount");
                        results.put("literalCount", literalCount + 1);
                        
                        
                    } else {
                        
                        //TODO if we are creating a dictionary why unscape this term anyway?
                        //term = NxUtil.unescape(nodes[i].toN3());
                        term = nodes[i].toN3();

                        if(nodes[i] instanceof BNode) {
                            blankNodes.add(term);

                        } else {
                           // try {                            
                            //root.addTerm(term);
                           /* } catch(IndexOutOfBoundsException e) {
                                System.out.println(term);
                                throw e;
                            }*/
                            //resources.add(term);
                            
                            if(!SchemaURIType.RDF_OWL_TERMS.contains(term)) {
                                
                               List<String> parts = TermUtils.split(term);
                               // String [] parts = StringUtils.split(path, TermRoot.URI_SEP);
                                
                                // invalid URIs, e.g. <http:///www.taotraveller.com> is parsed by NxParser as http:///
                                /*if(parts.length > 0) {
                                   Collections.addAll(resources, parts); 
                                }*/
                                if(!parts.isEmpty()) {
                                    resources.addAll(parts);
                                }
                                
                                if(this.maxParts < parts.size()) {
                                    this.maxParts = parts.size();
                                    results.put("maxParts", this.maxParts);
                                    System.out.println(term);
                                }
                            }

                        }
                        
                        if(counter != null) {
                            counter.count(term);
                        }
                    }
                }
   
                return null;
            }

            @Override
            public void setCounter(TermCounter counter) {   
                this.counter = counter;
            }
            
        };
        
        callback.setCounter(counter);
        processor.read(callback);
        
        
        /*for(Entry<String, Integer> uri: uris.entrySet()) {
            if(uri.getValue() > 10) {
                System.out.println(uri.getKey() + " --------- " + uri.getValue());
            }
        }*/
        
        try(PrintWriter writer = new PrintWriter(new FileOutputStream(Utils.TEMP_FOLDER + "term_root2.txt"))) {
            System.out.println("Unique hostnames found in the datasets = " + root.size());
            for(TermPart part: root.values()) {
                writer.println("+ " + part.getTerm() + " ------- " + part.size());
                print(part, false, new StringBuilder("  "), writer);
            }
        }
        
        System.out.println("Maximum number of URI parts: " + results.get("maxParts"));
        System.out.println("Number of blank nodes found: " + blankNodes.size());
        System.out.println("Number of unique resources parts found: " + resources.size());

        System.out.println("First 10 shortened resources: ");
        int i = 0;

        for(String resource: resources) {

            i++;
            System.out.println(resource);
            if(i == 10) {
                break;
            }
        }
        
        //System.out.println("TermRoot: " + root);
        
        Utils.objectToFile(Utils.TEMP_FOLDER + "term_root2.kryo.gz", resources, true, false);
        
        
    }
    
    private static void print(TermPart part, boolean printCurrent, StringBuilder indent, PrintWriter writer) {
        
        indent = new StringBuilder(indent.toString());
        
        if(part.hasChildren()) {
            if(printCurrent) {
                writer.println(indent.toString() + part.getTerm() + " ------- " + part.size());
            }

            indent.append("  ");
            
            for(TermPart child: part.values()) {
                print(child, true, indent, writer);
            }
            
        } else if(printCurrent){
            writer.println(indent.toString() + part.getTerm());
        }
    }
    
    private static void testDictionary() throws IOException, ClassNotFoundException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String filename = "/Users/omerio/Ontologies/dbpedia/instance_types_en.nt.gz";//"/Users/omerio/Ontologies/swetodblp_2008_8.nt.gz";
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
        /*Stopwatch stopwatch1 = Stopwatch.createStarted();
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
        System.out.println(id);*/
        
    }

    /**
     * 
     * @param args
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        //testDictionary();
        //"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        /*System.out.println("Starting");
        
        
        String treeFile = Utils.TEMP_FOLDER + "term_root1.kryo.gz";
        TermRoot root = Utils.objectFromFile(treeFile, TermRoot.class, true, false);
        
        System.out.println(root.getTerms().get("www.eurohandball.com"));
        System.out.println(root.size());
        */
        
        testExtratCommonURIs();
        
        /*TermRoot root = new TermRoot();
        root.addTerm("<http://www.eurohandball.com/ech/men/2014/match/2/052/Netherlands+-+Sweden>");
        System.out.println(root.getTerms().get("www.eurohandball.com").get("ech").get("men").get("2014").get("match").get("2").get("052"));*/
        
        //String term = "<http://en.wikisource.org/>";
        //String term = "<http://en.wikisource.org/w/index.php?title=User:Tim_Starling/ScanSet_TIFF_demo&vol=02&page=EB2A196>";
        /*String term = "<http://a/d>";
        
        String path = term.substring(1, term.length() - 1);
        String uri = StringUtils.substringBeforeLast(path, "/");

        System.out.println(uri);
        if(uri.length() > 7 && uri.length() + 1 < path.length()) {

            System.out.println(StringUtils.remove(path, uri + "/"));
        }*/
        
        
        /*StringBuilder sb = new StringBuilder();
        for (int i = 100000; i < 100000 + 60; i++)
            sb.append(i).append(' ');*/
        /*String sample = "www.eurohandball.com/ech/men/2014/match/2/052/Netherlands+-+Sweden";//sb.toString();

        int runs = 100000;
        for (int i = 0; i < 5; i++) {
            {
                long start = System.nanoTime();
                for (int r = 0; r < runs; r++) {
                    StringTokenizer st = new StringTokenizer(sample, "/");
                    List<String> list = new ArrayList<String>();
                    while (st.hasMoreTokens())
                        list.add(st.nextToken());
                }
                long time = System.nanoTime() - start;
                System.out.printf("StringTokenizer took an average of %.1f us%n", time / runs / 1000.0);
            }
            {
                long start = System.nanoTime();
               
                for (int r = 0; r < runs; r++) {
                    List<String> list = Arrays.asList(sample.split("/"));
                }
                long time = System.nanoTime() - start;
                System.out.printf("String.split took an average of %.1f us%n", time / runs / 1000.0);
            }
            {
                long start = System.nanoTime();
                for (int r = 0; r < runs; r++) {
                    List<String> list = Utils.split(sample, '/');
                }
                long time = System.nanoTime() - start;
                System.out.printf("indexOf loop took an average of %.1f us%n", time / runs / 1000.0);
            }
            
            {
                long start = System.nanoTime();
                
                for (int r = 0; r < runs; r++) {
                    List<String> list = Arrays.asList(StringUtils.split(sample, "/"));
                }
                long time = System.nanoTime() - start;
                System.out.printf("StringUtils.split took an average of %.1f us%n", time / runs / 1000.0);
            }
            
            System.out.println("-----------------------------------------------");
         }
         */

    }
    
    

}
