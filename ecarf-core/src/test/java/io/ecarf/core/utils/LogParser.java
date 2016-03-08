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

import io.cloudex.cloud.impl.google.compute.GoogleMetaData;
import io.cloudex.framework.cloud.entities.StorageObject;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

/**
 * Parses coordinator & processor log files, must end with .log
 * @author Omer Dawelbeit (omerio)
 *
 */
public class LogParser {
    
    private static final Map<String, TimeUnit> UNITS = new HashMap<>();
    
    static {
        
        UNITS.put("ns", TimeUnit.NANOSECONDS);
        UNITS.put("\u03bcs", TimeUnit.MICROSECONDS);
        UNITS.put("ms", TimeUnit.MILLISECONDS);
        UNITS.put("s", TimeUnit.SECONDS);
        UNITS.put("min", TimeUnit.MINUTES);
        UNITS.put("h", TimeUnit.HOURS);
        UNITS.put("d", TimeUnit.DAYS);

    }
    
    private static final String PROCESSOR = "processor";
    
    private static final String COORDINATOR = "coordinator";
    
    private static final String TIMER = " TIMER# Task io.ecarf.core.cloud.task.";
    private static final String COMPLETED_IN = " completed in";
    private static final String DUMMY_TASK = "processor.DummyProcessorTask";
    private static final String EXTRACT_TASK = "processor.analyze.ExtractCountTerms2PartTask";
    private static final String ASSEMBLE_DICT_TASK = "processor.dictionary.AssembleDictionaryTask";
    private static final String PROCESS_LOAD_TASK = "processor.ProcessLoadTask";
    private static final String LOAD_TASK = "coordinator.LoadBigDataFilesTask";
    private static final String DO_REASON_TASK = "processor.reason.phase2.DoReasonTask9";
    
    private static final String REASON_TASK_SUMMARY = " [main] reason.phase2.DoReasonTask9 - ";
    
    private static final String ELAPSED_JOB = "framework.components.Coordinator - TIMER# Job elapsed time:";
    
    private static final String BIGQUERY_SAVE = "[main] impl.google.GoogleCloudServiceImpl - BigQuery query data saved successfully, timer:";
    
    private static final String BIGQUERY_JOB_ELAPSED = "[main] impl.google.GoogleCloudServiceImpl - Job Status: DONE, elapsed time (secs): ";
    
    private static final String ASSEMBLE_DICTIONARY_SUBTASK = "processor.dictionary.AssembleDictionarySubTask";
    
    private static final String TERM_DICT_CON = "term.dictionary.TermDictionaryConcurrent";
    
    private static final String TERM_DICT = "term.dictionary.TermDictionary - TIMER#";
    
    private static final String JSON_NUM_VM = "\"numberOfProcessors\":";
    private static final String JSON_VM_TYPE = "\"vmType\":";
    
    private static final String FILE_ITEMS = "processor.files.ProcessFilesTask - Processing files: ";
    
    private EcarfGoogleCloudServiceImpl service;
    
    private Set<String> files = new HashSet<>();
    
    //private List<Double> jobElapsedTimes = new ArrayList<>();
       
    private List<CoordinatorStats> coordinators = new ArrayList<>();
    
    private List<ProcessorStats> processors = new ArrayList<>();
    
    private List<DictionaryStats> dictionaries = new ArrayList<>();
    
    
    
    /**
     * @throws Exception 
     * 
     */
    public LogParser(String folder) throws Exception {
        super();
        
        boolean remote = folder.startsWith(GoogleMetaData.CLOUD_STORAGE_PREFIX);
        
        if(remote) {
            
            String bucket = StringUtils.remove(folder, GoogleMetaData.CLOUD_STORAGE_PREFIX);
            this.setUp();
            List<StorageObject> objects = this.service.listCloudStorageObjects(bucket);
            
            for(StorageObject object: objects) {
                String name = object.getName();
                if(name.endsWith(Constants.DOT_LOG)) {
                    String localFile = FilenameUtils.getLocalFilePath(name);
                    
                    service.downloadObjectFromCloudStorage(name, localFile, bucket);
                    
                    this.files.add(localFile);
                }
            }
            
        } else {
            // local file
            DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
                public boolean accept(Path file) throws IOException {
                    
                    String filename = file.toString(); 
                    return filename.endsWith(Constants.DOT_LOG) && filename.contains(PROCESSOR) || filename.contains(COORDINATOR);
                }
            };
            
            Path dir = Paths.get(folder);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, filter)) {
                for (Path path : stream) {
                    this.files.add(path.toString());
                }
            }
        }
        
    }

    public void setUp() throws Exception {
        this.service = new EcarfGoogleCloudServiceImpl();
        TestUtils.prepare(service);
    }
    
    /**
     * i.e TIMER# Task io.ecarf.core.cloud.task.processor.DummyProcessorTask completed in 1.808 min
     * Process the timer on tasks
     * @param stats
     * @param line
     */
    private void parseTaskTimer(Stats stats, String line, boolean coordinator) {
        
        double timer = this.extractAndGetTimer(line, COMPLETED_IN);
        
        if(coordinator && line.indexOf(TIMER + DUMMY_TASK + COMPLETED_IN) > -1) { 
             ((CoordinatorStats) stats).evmAcquis = timer;

        } else if(coordinator && line.indexOf(TIMER + LOAD_TASK) > -1) {
            
            ((CoordinatorStats) stats).bigQueryLoad = timer;

        } else if(line.indexOf(TIMER + EXTRACT_TASK) > -1) {
            stats.extractCountTerms = timer;

        } else if(line.indexOf(TIMER + ASSEMBLE_DICT_TASK) > -1) {
            stats.assembleDictionary = timer;

        } else if(line.indexOf(TIMER + PROCESS_LOAD_TASK) > -1) {
            stats.processLoad = timer;

        } else if(line.indexOf(TIMER + DO_REASON_TASK) > -1) {

            stats.reasonPhase = timer;

        }
    }
    
        
    private static final String R_INFERRED = "Finished reasoning, total inferred triples = ";
    private static final String R_ROWS = "Total rows retrieved from big data = ";
    private static final String R_GBYTES = "Total processed GBytes = ";
    private static final String R_SERIAL_IN_FILE = "Total process reasoning time (serialization in inf file) = ";
    private static final String R_EMPTY_CYCLE = "Total time spent in empty inference cycles = ";
    
    
    /**
     * 03/03/2016 09:53:42 [ INFO] [main] reason.phase2.DoReasonTask9 - Finished reasoning, total inferred triples = 19535679
       03/03/2016 09:53:42 [ INFO] [main] reason.phase2.DoReasonTask9 - Total rows retrieved from big data = 30457343
       03/03/2016 09:53:42 [ INFO] [main] reason.phase2.DoReasonTask9 - Total processed GBytes = 10.054763808846474
       03/03/2016 09:53:42 [ INFO] [main] reason.phase2.DoReasonTask9 - Total process reasoning time (serialization in inf file) = 1.017 min
       03/03/2016 09:53:42 [ INFO] [main] reason.phase2.DoReasonTask9 - Total time spent in empty inference cycles = 1.508 min
     */
    private void extractProcessorReasoningStats(String line, ProcessorStats stats) {
        
        if(line.indexOf(R_INFERRED) > -1) {
            stats.inferred = Integer.parseInt(StringUtils.substringAfter(line, R_INFERRED));
            
        } else if(line.indexOf(R_ROWS) > -1) {
            stats.bigQueryRows = Integer.parseInt(StringUtils.substringAfter(line, R_ROWS));
            
        } else if(line.indexOf(R_GBYTES) > -1) {
            stats.bigQueryProcBytes = Double.parseDouble(StringUtils.substringAfter(line, R_GBYTES));
            
        } else if(line.indexOf(R_SERIAL_IN_FILE) > -1) {
            stats.serialInFile = this.extractAndGetTimer(line, R_SERIAL_IN_FILE);
            
        } else if(line.indexOf(R_EMPTY_CYCLE) > -1) {
            stats.retries = this.extractAndGetTimer(line, R_EMPTY_CYCLE);
        }
    }

    private double extractAndGetTimer(String line, String after) {
        return this.extractAndGetTimer(line, after, false);
    }
    
    private double extractAndGetTimer(String line, String after, boolean ignoreMillis) {
        String timer = StringUtils.substringAfter(line, after);
        timer = StringUtils.remove(timer, ':');
        timer = StringUtils.trim(timer);
        
        return this.parseStopwatchTime(timer, ignoreMillis); 
    }

    
    /**
     *  - Processing file: /tmp/wordnet_links.nt.gz.kryo.gz, dictionary items: 49382611, memory usage: 14.336268931627274GB, timer: 290.0 ms
     * /tmp/wikipedia_links_en.nt.gz.kryo.gz, dictionary items: 44, memory usage: 0.013648882508277893GB, timer: 2.636 s
     *                      START: Downloading file: interlanguage_links_chapters_en.nt.gz.kryo.gz, memory usage: 0.0GB
     * @param line
     * @param after
     * @return
     */
    private double [] extractAndGetMemoryDictionaryItems(String line) {
        double memory = 0;
        double items = 0;
        String memoryStr = null;
        
        if(line.contains(TIMER_PREFIX)) {
            memoryStr = StringUtils.substringBetween(line, MEM_USE, TIMER_PREFIX);
            
            if(line.contains(DIC_ITEMS)) {
                String itemsStr = StringUtils.trim(StringUtils.substringBetween(line, DIC_ITEMS, MEM_USE));

                items = Double.parseDouble(itemsStr);
            }
            
        } else {
            memoryStr = StringUtils.substringAfter(line, MEM_USE);
        }
        
        if(memoryStr != null) {
            memoryStr = StringUtils.remove(memoryStr, "GB");
            memoryStr = StringUtils.strip(memoryStr);
        }
        
        memory = Double.parseDouble(memoryStr);
        
        double [] values = new double [] {memory, items};
        return values;
    }
    
    /**
     * processor.dictionary.AssembleDictionarySubTask - Dictionary size: 44817045
     * @param line
     * @return
     */
    private int extractDictionarySize(String line) {
        return Integer.parseInt(StringUtils.substringAfter(line, DIC_SIZE));
    }
      
    private static final String TIMER_PREFIX = ", timer:";  
    private static final String MEM_USE = ", memory usage:";
    private static final String DIC_SIZE = "Dictionary size: ";
    private static final String DIC_ITEMS = "dictionary items: ";
    
    private static final String DIC_ASSEMBLE = "Successfully assembled dictionary with size: ";
    private static final String MAX_RES_ID = ", max resourceId: ";
    
    private static final String NON_CON_TIMER = "#TIMER finished creating non concurrent dictionary";
    private static final String TERM_DIC_TIMER = "TIMER# serialized dictionary to file:";
    //private static final String SERIAL_DICT = "Successfully serialized dictionary with size: ";
    private static final String SCHEMA_TERMS = "Schema terms added to the dictionary, final size: ";
    private static final String TERM_PARTS = " term parts ";
    private static final String PROCESSING = "- Processing: ";
    
    /**
     * Dictionary
     * @param dStats
     * @param line
     */
    private void extractDictionaryStats(DictionaryStats dStats, String line) {
        
        if(line.contains(MEM_USE)) {
            double[] values = this.extractAndGetMemoryDictionaryItems(line);
            double memory = values[0];
            dStats.memoryFootprint.add(memory);
            
            if(values[1] > 0) {
                dStats.memoryUsage.add(new MemUsage((int) values[1], memory));
            }
        
        } else if(line.contains(DIC_SIZE)) {
            int size = this.extractDictionarySize(line);
            
            dStats.memoryUsage.add(new MemUsage(size, dStats.getLatestMemoryUsage()));
        
        } 
        
        if(line.contains(DIC_ASSEMBLE)) {
            //Successfully assembled dictionary with size: 53550116, max resourceId: 54291281, memory usage: 14.449545934796333GB, timer: 5.010 min
            dStats.items = Integer.parseInt(StringUtils.substringBetween(line, DIC_ASSEMBLE, MAX_RES_ID));
            dStats.maxResourceId = Integer.parseInt(StringUtils.substringBetween(line, MAX_RES_ID, MEM_USE));
            dStats.assemble = this.extractAndGetTimer(line, TIMER_PREFIX);
        }
        
        /**
         * term.dictionary.TermDictionaryConcurrent - Creating non concurrent dictionary, memory usage: 11.166048146784306
           term.dictionary.TermDictionaryConcurrent - #TIMER finished creating non concurrent dictionary, memory usage: 13.966991074383259GB, timer: 1.577 min
           processor.dictionary.AssembleDictionaryTask - Successfully created non concurrent dictionary for serialization, memory usage: 13.966991074383259GB, timer: 6.992 min
           core.utils.Utils - Serializing object of class: class io.ecarf.core.term.dictionary.TermDictionaryCore to file: /tmp/dbpedia_dictionary_8c.kryo.gz, with compress = true
           term.dictionary.TermDictionary - TIMER# serialized dictionary to file: /tmp/dbpedia_dictionary_8c.kryo.gz, in: 3.274 min
           processor.dictionary.AssembleDictionaryTask - Successfully serialized dictionary with size: 53550116, memory usage: 13.964397609233856GB, timer: 10.27 min
         */
        
        if(line.contains(NON_CON_TIMER)) {
            dStats.nonConcurrent = this.extractAndGetTimer(line, TIMER_PREFIX);
        
        } else if(line.contains(TERM_DIC_TIMER)) {
            dStats.serialize = this.extractAndGetTimer(line, " in:");
            
        } else if(line.contains(SCHEMA_TERMS)) {
            //processor.dictionary.AssembleDictionaryTask - Schema terms added to the dictionary, final size: 53550784 , memory usage: 14.449545934796333GB
            dStats.itemsAfterSchema = Integer.parseInt(StringUtils.trim(StringUtils.substringBetween(line, SCHEMA_TERMS, MEM_USE)));
        }
        //processor.dictionary.AssembleDictionarySubTask - Processing: 1718527 term parts , memory usage: 17.80723436176777GB, timer: 4.839 s
        if(line.contains(TERM_PARTS)) {
            dStats.parts += Integer.parseInt(StringUtils.trim(StringUtils.substringBetween(line, PROCESSING, TERM_PARTS)));
        }

    }

    
    /**
     * 
     * @throws FileNotFoundException
     * @throws IOException
     */
    private void parse() throws FileNotFoundException, IOException {
        System.out.println("Parsing log files: ");
        
        for(String file: files) {
            
            boolean coordinator = file.contains(COORDINATOR);
            
            Stats stats;
            Stats dStats = null;
            
            List<Double> bigQuerySave = null;
            List<Double> bigQueryLoad = null;
            List<Double> bigQueryQueriesElapsed = null;
            
            if(coordinator) {
                stats = new CoordinatorStats();
                this.coordinators.add((CoordinatorStats) stats);
                
            } else {
                stats = new ProcessorStats();
                this.processors.add((ProcessorStats) stats);
                bigQuerySave = new ArrayList<>();
                bigQueryLoad = new ArrayList<>();
                bigQueryQueriesElapsed = new ArrayList<>();
                
                dStats = new DictionaryStats();
                dStats.filename = StringUtils.substringAfterLast(file, "/");
                this.dictionaries.add((DictionaryStats) dStats);
            }
            
            stats.filename = StringUtils.substringAfterLast(file, "/");
            
            //System.out.println(file);
            String line = null;
            
            try (BufferedReader r = new BufferedReader(new FileReader(file))) {
                do {
                    
                    line = r.readLine();
                    
                    if(line != null) {

                        if(line.indexOf(TIMER) > -1) {
                            this.parseTaskTimer(stats, line, coordinator);
                        
                        } else if(line.indexOf(ELAPSED_JOB) > -1 && coordinator) {
                            ((CoordinatorStats) stats).endToEnd = this.extractAndGetTimer(line, ELAPSED_JOB);
                        
                        } else if(line.indexOf(REASON_TASK_SUMMARY) > -1) {
                            this.extractProcessorReasoningStats(line, (ProcessorStats) stats);
                        
                        } else if(line.indexOf(BIGQUERY_SAVE) > -1) {
                            bigQuerySave.add(this.extractAndGetTimer(line, BIGQUERY_SAVE, true));
                        
                        } else if(line.indexOf(BIGQUERY_JOB_ELAPSED) > -1) {
                            r.readLine();
                            String line1 = r.readLine();
                            if(line1 != null && line1.indexOf("\"configuration\" : {") > -1) {
                                
                                line1 = r.readLine();
                                
                                if(line1.indexOf("\"load\" : {") > -1) {
                                    bigQueryLoad.add(this.extractAndGetTimer(line, BIGQUERY_JOB_ELAPSED, true));
                                    
                                } else if(line1.indexOf("\"query\" : {") > -1) {
                                    
                                    // fast forward to this line
                                    //"recordsWritten" : "0",
                                    
                                    do {
                                        line1 = r.readLine(); 
                                    } while (line1 != null && !line1.contains("\"recordsWritten\" :"));
                                    
                                    if(line1 != null && !line1.contains("\"recordsWritten\" : \"0\",")) {
                                        double value = this.extractAndGetTimer(line, BIGQUERY_JOB_ELAPSED, true);
                                        if(value > 0) {
                                            bigQueryQueriesElapsed.add(value);
                                        }
                                    }
                                    
                                } else if(line1.indexOf("\"extract\" : {") > -1) {
                                    
                                }
                            }
                        } else if(line.indexOf(ASSEMBLE_DICTIONARY_SUBTASK) > -1 || 
                                line.contains(ASSEMBLE_DICT_TASK) ||
                                line.contains(TERM_DICT_CON) ||
                                line.contains(TERM_DICT)) {
                            this.extractDictionaryStats((DictionaryStats) dStats, line);
                            
                        } else if(coordinator) {
                            if(line.contains(JSON_NUM_VM)) {
                                //"numberOfProcessors": 8.0
                                ((CoordinatorStats) stats).numOfProcessors = (int) Double.parseDouble(StringUtils.substringAfter(line, JSON_NUM_VM + " "));
                                
                                
                            } else if(line.contains(JSON_VM_TYPE)) {
                              //"vmType": "n1-standard-2",
                                ((CoordinatorStats) stats).vmType = StringUtils.substringBetween(line, JSON_VM_TYPE + " \"", "\",");
                            }
                            
                            
                        } else if(!coordinator && line.contains(FILE_ITEMS)) {
                            // line occurs twice per file, so only add if hasn't been added yet
                            if(((ProcessorStats) stats).fileItems.isEmpty()) {
                                String items = StringUtils.substringAfter(line, FILE_ITEMS);
                                //processor.files.ProcessFilesTask - Processing files: [revision_ids_en.nt.gz, revision_uris_en.nt.gz, yago_taxonomy.nt.gz, interlanguage_links_chapters_en.nt.gz, geo_coordinates_en.nt.gz]
                                List<String> fileItems = Lists.newArrayList(StringUtils.substringBetween(items, "[", "]").split(", "));
                                ((ProcessorStats) stats).fileItems.addAll(fileItems);
                            }
                        }

                    }

                } while(line != null);
            }
            
            if(!coordinator) {
                ((ProcessorStats) stats).bigQuerySave = this.sum(bigQuerySave);
                ((ProcessorStats) stats).bigQueryInsert = this.sum(bigQueryLoad);
                if(!bigQueryQueriesElapsed.isEmpty()) {
                    ((ProcessorStats) stats).bigQueryAverageQuery = this.sum(bigQueryQueriesElapsed) / bigQueryQueriesElapsed.size();
                }
            }
            
            
        }
        
        //if(!this.jobElapsedTimes.isEmpty()) {
         //   this.coordinators.get(0).endToEnd = this.jobElapsedTimes.get(this.jobElapsedTimes.size() - 1);
        //}
        
    }

    private double sum(List<Double> values) {
        double sum = 0;
        for(double value: values) {
            sum += value;
        }
        return sum;
    }

    /**
     * return the value in minutes
     * @param timer
     * @return
     */
    private double parseStopwatchTime(String timer, boolean ignoreMillis) {
        
        String [] parts = timer.split(" ");
        
        
        TimeUnit unit = UNITS.get(StringUtils.trim(parts[1]));
        
        double value = Double.parseDouble(StringUtils.trim(parts[0]));
        
        switch(unit) {
        case DAYS:
            value = value * 24 * 60;
            break;
            
        case HOURS:
            value = value * 60;
            break;
            
        case MICROSECONDS:
            value = 0;
            break;
            
        case MILLISECONDS:
            
            if(ignoreMillis) {
                value = 0;
                
            } else {
                value = value / (1000 * 60);
            }
                
            break;
            
        case MINUTES:
            
            break;
            
        case NANOSECONDS:
            value = 0;
            break;
            
        case SECONDS:
            value = value / 60;
            break;
        default:
            throw new IllegalArgumentException("Not found" + unit);
        
        }

        return value;
         
    }
    
    /**
     * 
     */
    public void printStats() {

        System.out.println("------------------------------------------------ Coordinators ---------------------------------------");
        
        for(CoordinatorStats coordinator: this.coordinators) {
            System.out.println(CoordinatorStats.HEADER);
            System.out.println(coordinator);
            System.out.println();
        }
        
        // Marry up processors containing the same items, may be multiple runs
        Map<String, Set<ProcessorStats>> joined = new HashMap<>();

        System.out.println("------------------------------------------------ Processors ---------------------------------------");
        Collections.sort(processors);
        System.out.println();
        System.out.println(ProcessorStats.HEADER);
        for(ProcessorStats processor: processors) {
            System.out.println(processor);
            
            if(!processor.fileItems.isEmpty()) {
                String fileItem = processor.fileItems.toString();
                
                if(joined.containsKey(fileItem)) {
                    joined.get(fileItem).add(processor);
                
                } else {
                    Set<ProcessorStats> stats = new HashSet<>();
                    stats.add(processor);
                    joined.put(fileItem, stats);
                }
            }
        }
        
        System.out.println();
        System.out.println("------------------------------------------------ Joined Processors ---------------------------------------");
        
        StringBuilder joinedUp = new StringBuilder("File items,ExtractCountTerms2PartTask,ProcessLoadTask\n");
        
        for(Entry<String, Set<ProcessorStats>> entry: joined.entrySet()) {
            Set<ProcessorStats> values = entry.getValue();
            if(values.size() > 1) {
                System.out.println("------------------------------------------------ Joined up multi-experiment processors with same file items ---------------------------------------");
                System.out.println("Filename, File items,ExtractCountTerms2PartTask,ProcessLoadTask");
                double extractCountTermsAvg = 0;
                double processLoadAvg = 0;
                String fileItems = entry.getKey().replace(',', ';');
                for(ProcessorStats stats: values) {
                    System.out.println(stats.filename + ',' + fileItems + ',' + stats.extractCountTerms + ',' + stats.processLoad);
                    
                    extractCountTermsAvg += stats.extractCountTerms;
                    processLoadAvg += stats.processLoad;
                }
                
                extractCountTermsAvg = extractCountTermsAvg / values.size();
                processLoadAvg = processLoadAvg / values.size();
                
                // averages
                joinedUp.append(fileItems).append(',').append(extractCountTermsAvg).append(',').append(processLoadAvg).append('\n');
                

                System.out.println();
            }
        }
        
        System.out.println(joinedUp.toString());
        
        System.out.println();
        System.out.println("------------------------------------------------ Dictionary ---------------------------------------");
        for(DictionaryStats dictionary: this.dictionaries) {
            System.out.println(DictionaryStats.HEADER);
            System.out.println(dictionary);
            //System.out.println();
            System.out.print(dictionary.getMemoryStatsString());
            System.out.println();
        }
    }

    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String [] args) throws Exception {
        if(args.length != 1) {
            System.out.println("Usage: LogParser gs://<bucket> or LogParser /path/to/directory");
            System.exit(-1);
        }
        
        String path = args[0];
        
        LogParser parser = new LogParser(path);
        parser.parse();
        parser.printStats();
        
        
    }
    
    
    public class Stats implements Comparable<Stats>  {
        String filename;
        
        double extractCountTerms;
        
        double assembleDictionary;
        
        double processLoad;
        
        double reasonPhase;

        @Override
        public int compareTo(Stats o) {
            
            return this.filename.compareTo(o.filename);
        }
    }
    
    public class MemUsage {
        
        int parts;
        
        double memory;

        /**
         * @param parts
         * @param memory
         */
        public MemUsage(int parts, double memory) {
            super();
            this.parts = parts;
            this.memory = memory;
        }
        
        
    }
    
    public class DictionaryStats extends Stats {
        
        int parts;
        
        int itemsAfterSchema;
        
        int items;
        
        int maxResourceId;
        
        double assemble;
        
        List<Double> memoryFootprint = new ArrayList<>();
        
        double nonConcurrent;
        
        double serialize;
        
        double upload;
        
        List<MemUsage> memoryUsage = new ArrayList<>();
        
        /**
         * 
         * @return
         */
        public double getLatestMemoryUsage() {
            double memory = 0;
            if(!memoryFootprint.isEmpty()) {
                memory = this.memoryFootprint.get(this.memoryFootprint.size() - 1);
            }
            return memory;
        }
        
        static final String HEADER = 
                "Filename, Parts, Dictionary Items After Schema, Dictionary Items,Max Resource Id, Assemble Time (min),Min memory (GB), "
                + "Max memory(GB),Create Non Concurrent(sec), Serialize Time(min), Upload Time (sec)";
        
        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            
            double minMem = 0;
            double maxMem = 0;
            
            if(!this.memoryFootprint.isEmpty()) {
                Collections.sort(this.memoryFootprint);
                
                minMem = this.memoryFootprint.get(0);
                maxMem = this.memoryFootprint.get(this.memoryFootprint.size() - 1);
            }
            return new StringBuilder(filename).append(',')
                    .append(parts).append(',')
                    .append(itemsAfterSchema).append(',')
                    .append(items).append(',')
                    .append(maxResourceId).append(',')
                    .append(assemble).append(',')
                    .append(minMem).append(',')
                    .append(maxMem).append(',')
                    .append(nonConcurrent * 60).append(',')
                    .append(serialize).append(',')
                    .append(upload * 60)
                    .toString();
        }
        
        /**
         * 
         * @return
         */
        public String getMemoryStatsString() {
            StringBuilder memory = new StringBuilder("Dictionary Size, Memory\n");
            for(MemUsage usage: this.memoryUsage) {
                
                memory.append(usage.parts).append(',').append(usage.memory).append('\n');
                
            }
            return memory.toString();
        }
        
    }
    
    public class CoordinatorStats extends Stats {
        
        double evmAcquis;
        
        //double loadPhase;
        
        double bigQueryLoad;
        
        double endToEnd;
        
        String vmType;
        
        int numOfProcessors;
        
        //List<Double> jobElapsedTimes = new ArrayList<>();
        
        static final String HEADER = 
                "Filename,VM Type, No of VMs,EVM acquisition time (min),ExtractCountTerms2PartTask,AssembleDictionaryTask,ProcessLoadTask,"
                + "Loading Phase,Bigquery load time (sec),Reasoning Phase (min),End to End (min)";

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return new StringBuilder(filename).append(',')
                    .append(vmType).append(',')
                    .append(numOfProcessors).append(',')
                    .append(evmAcquis).append(',')
                    .append(extractCountTerms).append(',')
                    .append(assembleDictionary).append(',')
                    .append(processLoad).append(',')
                    .append(',')
                    .append(bigQueryLoad * 60).append(',')
                    .append(reasonPhase).append(',')
                    .append(endToEnd)
                    .toString();
        }
        
        
    }
    
    
    public class ProcessorStats extends Stats {
                
        int inferred;
        
        int bigQueryRows;
        
        double bigQuerySave;
        
        double bigQueryProcBytes;
        
        double serialInFile;
        
        double retries;
        
        double bigQueryAverageQuery;
        
        double bigQueryInsert;
        
        List<String> fileItems = new ArrayList<>();
        
        static final String HEADER = "Filename,ExtractCountTerms2PartTask,AssembleDictionaryTask,ProcessLoadTask,Inferred,Retrieved Bigquery Rows,"
                + "Bigquery results save time (min),Big Query Table size GB,Big Query Table rows,Bigquery Total Bytes Processed (GB),"
                + "Node Reasoning Time(sec),8 retries with 10s sleep,Bigquery average Query time (sec),Bigquery Reason insert(min),Reasoning Phase (min)";
        
        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return new StringBuilder(filename).append(',')
                    .append(extractCountTerms).append(',')
                    .append(assembleDictionary).append(',')
                    .append(processLoad).append(',')
                    .append(inferred).append(',')
                    .append(bigQueryRows).append(',')
                    .append(bigQuerySave).append(',')
                    .append(',').append(',')
                    .append(bigQueryProcBytes).append(',')
                    .append(serialInFile * 60).append(',')
                    .append(retries).append(',')
                    .append(bigQueryAverageQuery * 60).append(',')
                    .append(bigQueryInsert).append(',')
                    .append(reasonPhase)
                    .toString();
        }

        
    }
    
}
