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


package io.ecarf.core.cloud.impl.google;

import io.cloudex.cloud.impl.google.GoogleCloudServiceImpl;
import io.cloudex.cloud.impl.google.bigquery.BigQueryStreamable;
import io.cloudex.framework.cloud.entities.BigDataTable;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.compress.NxGzipCallback;
import io.ecarf.core.compress.NxGzipProcessor;
import io.ecarf.core.compress.callback.StringEscapeCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class EcarfGoogleCloudServiceImpl extends GoogleCloudServiceImpl implements EcarfGoogleCloudService {


    /**
     * Convert the provided file to a format that can be imported to the Cloud Database
     * 
     * @param filename
     * @return
     * @throws IOException 
     */
    @Override
    public String prepareForBigQueryImport(String filename) throws IOException {
        return this.prepareForBigQueryImport(filename, null);
    }

    /**
     * Convert the provided file to a format that can be imported to the Cloud Database
     * 
     * @param filename
     * @return
     * @throws IOException 
     */
    @Override
    public String prepareForBigQueryImport(String filename, final TermCounter counter) throws IOException {
        /*String outFilename = new StringBuilder(FileUtils.TEMP_FOLDER)
              .append(File.separator).append("out_").append(filename).toString();*/
        NxGzipProcessor processor = new NxGzipProcessor(filename);
        
        NxGzipCallback callback = new StringEscapeCallback();

        callback.setCounter(counter);
        
        String outFilename = processor.process(callback);

        return outFilename;
    }
    
    /**
     * Download a json file from cloud storage which includes a JSON array and then parse it as a set
     * @param filename
     * @param bucket
     * @return
     * @throws IOException
     */
    @Override
    public Set<String> getSetFromCloudStorageFile(String filename, String bucket) throws IOException {
        Set<String> values = null;

        if(StringUtils.isNoneBlank(filename)) {
            String localFilename = Utils.TEMP_FOLDER + filename;
            this.downloadObjectFromCloudStorage(filename, localFilename, bucket);

            // convert from JSON
            values = FileUtils.jsonFileToSet(localFilename);
        } 
        
        return values;
    }

    /**
     * Stream local N triple files into big query
     * @param files
     * @param table
     * @throws IOException
     */
    @Override
    public void streamLocalFilesIntoBigData(List<String> files, BigDataTable table) throws IOException {
        Collection<? extends BigQueryStreamable> triples = null;
        for(String file: files) {
            triples = TripleUtils.loadNTriples(file);

            if(!triples.isEmpty()) {
                this.streamObjectsIntoBigData(triples, table);
            }
        }
    }

}
