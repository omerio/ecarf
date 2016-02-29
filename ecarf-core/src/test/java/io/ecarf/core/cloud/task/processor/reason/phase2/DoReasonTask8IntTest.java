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

import static org.junit.Assert.assertEquals;
import io.ecarf.core.cloud.impl.google.EcarfGoogleCloudServiceImpl;
import io.ecarf.core.triple.ETriple;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.TestUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DoReasonTask8IntTest {
    
    private EcarfGoogleCloudServiceImpl service;
    
    @Before
    public void setUp() throws Exception {
       
        this.service = new EcarfGoogleCloudServiceImpl();
        TestUtils.prepare(service);
    }

    /**
     * Test method for {@link io.ecarf.core.cloud.task.processor.reason.phase2.DoReasonTask7#run()}.
     * @throws IOException 
     */
    @Test
    
    public void testRun() throws IOException {
        DoReasonTask9 task = new DoReasonTask9();
        task.setCloudService(service);
        task.setBucket("dbpedia-fullrun-3");
        task.setTable("ontologies.dbpedia2");
        task.setSchemaFile("dbpedia_3.9_gen_closure_encoded.csv");
        task.setTerms("23055324594068088,18485755362496296,23055260239613800,23072920822418984,23056360843075176,23055259984027448,23059722439237224,23059997534259816,23061114226805544,23056652564238200,16331882755874496,23059659092664120");
        task.run();
    }
    
    
    @Test
    @Ignore
    public void testCsvParser() throws FileNotFoundException, IOException {
        
    
        String filename = "/var/folders/3h/0whnrhjn1ddfb5p9pq_c6_mh0000gn/T//ecarf-evm-1_1456690870927_QueryResults_0";
        int rows = 0;
        
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(new FileInputStream(filename), Constants.GZIP_BUF_SIZE)), Constants.GZIP_BUF_SIZE);) {

            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord().parse(reader);
            
            for (CSVRecord record : records) {

                ETriple instanceTriple = ETriple.fromCSV(record.values());
                rows++;
            }
        }
        
        assertEquals(8091263, rows);

    }
    

}
