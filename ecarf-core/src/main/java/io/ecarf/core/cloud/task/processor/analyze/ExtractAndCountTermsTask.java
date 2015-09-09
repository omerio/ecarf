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


package io.ecarf.core.cloud.task.processor.analyze;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.cloudex.framework.task.CommonTask;
import io.ecarf.core.cloud.task.processor.ProcessLoadTask1;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ExtractAndCountTermsTask extends CommonTask {
    
    
    private final static Log log = LogFactory.getLog(ExtractAndCountTermsTask.class);

    private String bucket;

    private String schemaTermsFile;

    private String files;

    /* (non-Javadoc)
     * @see io.cloudex.framework.Executable#run()
     */
    @Override
    public void run() throws IOException {
       
        log.info("Extracting all terms and counting schema terms");
    }

    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * @param bucket the bucket to set
     */
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    /**
     * @return the schemaTermsFile
     */
    public String getSchemaTermsFile() {
        return schemaTermsFile;
    }

    /**
     * @param schemaTermsFile the schemaTermsFile to set
     */
    public void setSchemaTermsFile(String schemaTermsFile) {
        this.schemaTermsFile = schemaTermsFile;
    }

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
