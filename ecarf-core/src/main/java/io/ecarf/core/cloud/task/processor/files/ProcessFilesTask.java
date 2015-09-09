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
package io.ecarf.core.cloud.task.processor.files;

import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.utils.ObjectUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

/**
 * Generic multi-threaded task that processes files provided to the processor component
 * in the metadata
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public abstract class ProcessFilesTask<T> extends CommonTask {

    private final static Log log = LogFactory.getLog(ProcessFilesTask.class);

    private String files;

   
    /* 
     * // TODO distinguish between files in cloud storage vs files downloaded from http or https url
     * (non-Javadoc)
     * @see io.ecarf.core.cloud.task.Task#run()
     */
    @Override
    public void run() throws IOException {

        log.info("START: processing files");

        Stopwatch stopwatch = Stopwatch.createStarted();


        Set<String> filesSet = ObjectUtils.csvToSet(files);
        log.info("Processing files: " + filesSet);

        List<Callable<T>> tasks = getSubTasks(filesSet);

        int processors = Runtime.getRuntime().availableProcessors();

        try {

            // check if we only have one file to process
            if(tasks.size() == 1) {

                this.processSingleOutput(tasks.get(0).call());

            } else if(processors == 1) {
                // only one process then process synchronously
                List<T> output = new ArrayList<>();
                for(Callable<T> task: tasks) {
                    output.add(task.call());
                }

                this.processMultiOutput(output);

            } else {

                // multiple cores
                ExecutorService executor = Utils.createFixedThreadPool(processors);

                try {

                    List<Future<T>> results = executor.invokeAll(tasks);
                    List<T> output = new ArrayList<>();

                    for (Future<T> result : results) {
                        output.add(result.get());
                    }

                    this.processMultiOutput(output);
                    
                } finally {
                    executor.shutdown();
                }
            }
            
        } catch(Exception e) {
            log.error("Failed to process multiple files", e);
            throw new IOException(e);

        }

        log.info("TIMER# All files are processed successfully, elapsed time: " + stopwatch);
    }
    
    /**
     * Get a list of callable tasks
     * @param files
     * @return
     */
    public abstract List<Callable<T>> getSubTasks(Set<String> files);

    /**
     * Process a single output of type T
     * @param output
     */
    public abstract void processSingleOutput(T output);

    /**
     * Process a list of output of type T
     * @param output
     */
    public abstract void processMultiOutput(List<T> output);

    /**
     * @return the files
     */
    public String getFiles() {
        return files;
    }

    /**
     * @param files the files to set
     */
    public void setFiles(String files) {
        this.files = files;
    }

}
