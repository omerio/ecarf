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


package io.ecarf.core.cloud.task.processor.reason.phase2;

import io.ecarf.core.reason.rulebased.GenericRule;
import io.ecarf.core.reason.rulebased.Rule;
import io.ecarf.core.triple.ETriple;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.triple.Triple;
import io.ecarf.core.utils.Constants;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class ReasonUtils {

    private final static Log log = LogFactory.getLog(ReasonUtils.class);

    /**
     * 
     * @param file
     * @param writer
     * @param compressed
     * @return
     * @throws IOException 
     */
    public static int reason(String inFile, String outFile, boolean compressed, 
            Map<Long, Set<Triple>> schemaTerms, Set<Long> productiveTerms) throws IOException {
        
        log.info("Reasoning for file: " + inFile);

        int inferredTriples = 0;

        // loop through the instance triples probably stored in a file and generate all the triples matching the schema triples set
        try (BufferedReader reader = getQueryResultsReader(inFile, compressed); 
             PrintWriter writer = 
                        new PrintWriter(new BufferedOutputStream(new GZIPOutputStream(
                                new FileOutputStream(outFile), Constants.GZIP_BUF_SIZE), Constants.GZIP_BUF_SIZE));) {

            Iterable<CSVRecord> records;

            if(compressed) {
                // ignore first row subject,predicate,object
                records = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord().parse(reader);

            } else {
                records = CSVFormat.DEFAULT.parse(reader);
            }

            Long term;

            for (CSVRecord record : records) {

                ETriple instanceTriple = ETriple.fromCSV(record.values());

                // TODO review for OWL ruleset
                if(SchemaURIType.RDF_TYPE.id == instanceTriple.getPredicate()) {

                    term = instanceTriple.getObject(); // object

                } else {

                    term = instanceTriple.getPredicate(); // predicate
                }

                Set<Triple> schemaTriples = schemaTerms.get(term);

                if((schemaTriples != null) && !schemaTriples.isEmpty()) {
                    productiveTerms.add(term);

                    for(Triple schemaTriple: schemaTriples) {
                        Rule rule = GenericRule.getRule(schemaTriple);
                        Triple inferredTriple = rule.head(schemaTriple, instanceTriple);

                        if(inferredTriple != null) {
                            writer.println(inferredTriple.toCsv());
                            inferredTriples++;
                        }
                    }
                }

            }

        }

        return inferredTriples;
    }

    /**
     * Get a reader based on if the query results are compressed or not
     * @param filename
     * @param compressed
     * @return
     * @throws IOException
     */
    private static BufferedReader getQueryResultsReader(String filename, boolean compressed) throws IOException {

        BufferedReader reader;

        if(compressed) {
            reader = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(filename), Constants.GZIP_BUF_SIZE)), Constants.GZIP_BUF_SIZE);

        } else {
            reader = new BufferedReader(new FileReader(filename), Constants.GZIP_BUF_SIZE);
        }

        return reader;
    }

}
