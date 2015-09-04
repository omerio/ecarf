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
import io.ecarf.core.compress.NTripleGzipCallback;
import io.ecarf.core.compress.NTripleGzipProcessor;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.triple.TripleUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;
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
        NTripleGzipProcessor processor = new NTripleGzipProcessor(filename);

        String outFilename = processor.process(new NTripleGzipCallback() {

            @Override
            public String process(String [] terms) {

                if(counter != null) {
                    counter.count(terms);
                }

                for(int i = 0; i < terms.length; i++) {
                    // bigquery requires data to be properly escaped
                    terms[i] = StringEscapeUtils.escapeCsv(terms[i]);
                }

                return StringUtils.join(terms, ',');
            }

        });

        return outFilename;
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
