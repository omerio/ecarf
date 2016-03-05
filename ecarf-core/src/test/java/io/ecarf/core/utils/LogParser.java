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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

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
    
    private EcarfGoogleCloudServiceImpl service;
    
    private Set<String> files = new HashSet<>();
    
    private List<Double> jobElapsedTimes = new ArrayList<>();
       
    private List<CoordinatorStats> coordinators = new ArrayList<>();
    
    private List<ProcessorStats> processors = new ArrayList<>();
    
    
    
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
    private void parseTaskTimer(Stats stats, String line) {
        
        double timer = this.extractAndGetTimer(line, COMPLETED_IN);
        
        if(line.indexOf(TIMER + DUMMY_TASK + COMPLETED_IN) > -1) { 
             ((CoordinatorStats) stats).evmAcquis = timer;

        } else if(line.indexOf(TIMER + LOAD_TASK) > -1) {
            
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
    
    private double sum(List<Double> values) {
        double sum = 0;
        for(double value: values) {
            sum += value;
        }
        return sum;
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
                
            }
            
            stats.filename = file;
            
            //System.out.println(file);
            String line = null;
            
            try (BufferedReader r = new BufferedReader(new FileReader(file))) {
                do {
                    
                    line = r.readLine();
                    
                    if(line != null) {

                        if(line.indexOf(TIMER) > -1) {
                            this.parseTaskTimer(stats, line);
                        
                        } else if(line.indexOf(ELAPSED_JOB) > -1) {
                            this.jobElapsedTimes.add(this.extractAndGetTimer(line, ELAPSED_JOB));
                        
                        } else if(line.indexOf(REASON_TASK_SUMMARY) > -1) {
                            this.extractProcessorReasoningStats(line, (ProcessorStats) stats);
                        
                        } else if(line.indexOf(BIGQUERY_SAVE) > -1) {
                            bigQuerySave.add(this.extractAndGetTimer(line, BIGQUERY_SAVE, true));
                        
                        } else if(line.indexOf(BIGQUERY_JOB_ELAPSED) > -1) {
                            r.readLine();
                            String line1 = r.readLine();
                            if(line1.indexOf("\"configuration\" : {") > -1) {
                                
                                line1 = r.readLine();
                                
                                if(line1.indexOf("\"load\" : {") > -1) {
                                    bigQueryLoad.add(this.extractAndGetTimer(line, BIGQUERY_JOB_ELAPSED, true));
                                    
                                } else if(line1.indexOf("\"query\" : {") > -1) {
                                    double value = this.extractAndGetTimer(line, BIGQUERY_JOB_ELAPSED, true);
                                    if(value > 0) {
                                        bigQueryQueriesElapsed.add(value);
                                    }
                                    
                                } else if(line1.indexOf("\"extract\" : {") > -1) {
                                    
                                }
                            }
                        }

                    }

                } while(line != null);
            }
            
            if(!coordinator) {
                ((ProcessorStats) stats).bigQuerySave = this.sum(bigQuerySave);
                ((ProcessorStats) stats).bigQueryInsert = this.sum(bigQueryLoad);
                ((ProcessorStats) stats).bigQueryAverageQuery = this.sum(bigQueryQueriesElapsed) / bigQueryQueriesElapsed.size();
            }
            
            
        }
        
        if(!this.jobElapsedTimes.isEmpty()) {
            this.coordinators.get(0).endToEnd = this.jobElapsedTimes.get(this.jobElapsedTimes.size() - 1);
        }
        
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

        for(CoordinatorStats coordinator: this.coordinators) {
            System.out.println(CoordinatorStats.HEADER);
            System.out.println(coordinator);
            System.out.println();
        }

        Collections.sort(processors);
        System.out.println();
        System.out.println(ProcessorStats.HEADER);
        for(ProcessorStats processor: processors) {
            System.out.println(processor);
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
    
    public class CoordinatorStats extends Stats {
        
        double evmAcquis;
        
        //double loadPhase;
        
        double bigQueryLoad;
        
        double endToEnd;
        
        static final String HEADER = 
                "Filename,EVM acquisition time (min),ExtractCountTerms2PartTask,AssembleDictionaryTask,ProcessLoadTask,"
                + "Loading Phase,Bigquery load time (sec),Reasoning Phase (min),End to End (min)";

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return new StringBuilder(filename).append(',')
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
