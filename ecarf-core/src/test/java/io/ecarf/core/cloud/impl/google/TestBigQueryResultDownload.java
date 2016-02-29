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

import io.cloudex.framework.cloud.entities.BigDataTable;
import io.cloudex.framework.cloud.entities.QueryStats;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.utils.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TestBigQueryResultDownload {
    
    private EcarfGoogleCloudServiceImpl service;
    
    private static final String QUERY = "select subject, predicate, object from ontologies.swetodblp3 where "
            + "(predicate=0 and object IN (305857022128,1195933142240,1125875632288,1113242393776,1182785047712,4412935998960,289499154672,1114044538032)) OR "
            + "(predicate IN (285450340512,96426690800,30638312928,1113323182256,374308002272,79293028576,1194864924832,95402494176,"
            + "27422070944,1177684974000,17877322877088,95151901920,1130502277552,1400733949360,1388133125296,1469738568160,"
            + "30705355936,4494473271456,27706181856,1456907984352));";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        this.service = new EcarfGoogleCloudServiceImpl();
        TestUtils.prepare(service);
    }

    /**
     * 28/02/2016 14:26:21 [DEBUG] [main] impl.google.GoogleCloudServiceImpl - Job completed successfully{
  "configuration" : {
    "query" : {
      "createDisposition" : "CREATE_IF_NEEDED",
      "destinationTable" : {
        "datasetId" : "_aeda0cdb9ddd1481c7e8d6b0414066988d7f5216",
        "projectId" : "ecarf-1000",
        "tableId" : "anon390646df30a72cc96bc742fc5c664adadaa52a03"
      },
      "query" : "select subject, predicate, object from ontologies.swetodblp1 where (predicate=0 and object IN (305857022128,1195933142240,1125875632288,1113242393776,1182785047712,4412935998960,289499154672,111404453803
2)) OR (predicate IN (285450340512,96426690800,30638312928,1113323182256,374308002272,79293028576,1194864924832,95402494176,27422070944,1177684974000,17877322877088,95151901920,1130502277552,1400733949360,1388133125296,1
469738568160,30705355936,4494473271456,27706181856,1456907984352));",
      "writeDisposition" : "WRITE_TRUNCATE"
    }
  },
  "etag" : "\"nwg3tKAm7RiC5vqWthFIuCNSGxs/De6uhmEQGBEl57rdF1lAcqObzyk\"",
  "id" : "ecarf-1000:job_xv-fLy1jvP19OKnXXfJbRZvhr9k",
  "jobReference" : {
    "jobId" : "job_xv-fLy1jvP19OKnXXfJbRZvhr9k",
    "projectId" : "ecarf-1000"
  },
  "kind" : "bigquery#job",
  "selfLink" : "https://www.googleapis.com/bigquery/v2/projects/ecarf-1000/jobs/job_xv-fLy1jvP19OKnXXfJbRZvhr9k",
  "statistics" : {
    "creationTime" : "1456669527271",
    "endTime" : "1456669579347",
    "query" : {
      "billingTier" : 1,
      "cacheHit" : false,
      "queryPlan" : [ {
        "computeRatioAvg" : 0.4500453371776173,
        "computeRatioMax" : 1.0,
        "id" : "1",
        "name" : "Stage 1",
        "readRatioAvg" : 0.12104159924805859,
        "readRatioMax" : 0.2423776564095538,
        "recordsRead" : "25553876",
        "recordsWritten" : "9078034",
        "steps" : [ {
          "kind" : "READ",
          "substeps" : [ "object, predicate, subject", "FROM ontologies.swetodblp1", "WHERE LOGICAL_OR(LOGICAL_AND(EQUAL(predicate, 0), ...), ...)" ]
        }, {
          "kind" : "WRITE",
          "substeps" : [ "object, predicate, subject", "TO __output" ]
        } ],
        "waitRatioAvg" : 0.007147217219182802,
        "waitRatioMax" : 0.007147217219182802,
        "writeRatioAvg" : 0.02543805967726963,
        "writeRatioMax" : 0.1305211924083758
      } ],
      "referencedTables" : [ {
        "datasetId" : "ontologies",
        "projectId" : "ecarf-1000",
        "tableId" : "swetodblp1"
      } ],
      "totalBytesBilled" : "596639744",
      "totalBytesProcessed" : "595863880"
    },
    "startTime" : "1456669528280",
    "totalBytesProcessed" : "595863880"
  },
  "status" : {
    "state" : "DONE"
  },
  "user_email" : "omer.dawelbeit@gmail.com"
}
     * @throws IOException
     */
    //@Test
    public void test() throws IOException {
        
        String jobId = service.startBigDataQuery(QUERY, new BigDataTable("ontologies.swetodblp3"));
        
        //String completedJob = service.checkBigQueryJobResults1(jobId, true, false);
        String token = "BFJ5QRJIKMAQAAASA4EAAEEAQCAAKGQIBCQI2BQQUCGQMIFQVYKQ====";
        
       // String jobId = "job_xv-fLy1jvP19OKnXXfJbRZvhr9k";
        
        String datasetId = "_aeda0cdb9ddd1481c7e8d6b0414066988d7f5216";
        
        String tableId = "anon390646df30a72cc96bc742fc5c664adadaa52a03";
        
        //GetQueryResultsResponse response = service.getQueryResults1(jobId, null);
                
        String bucket = "swetodblp-local";
        
        //service.extractTableToCloudStorageFile(bucket, datasetId, tableId);
        
        String filename = "swetodblp_table.csv";
        
        QueryStats stats = service.saveBigQueryResultsToCloudStorage(jobId, bucket, filename);
        
        System.out.println("Processed bytes: " + stats.getTotalProcessedGBytes() + "GB");
        System.out.println("Query result rows: " + stats.getTotalRows());
        
    }
    
    @Test
    public void testLargeQuery() throws IOException {
        /**
         * Downloading 61825926 rows from BigQuery for jobId: job_gW11WEy0LnT6ueL2-4NfkpgrNCQ
            29/02/2016 13:28:10 [DEBUG] [main] impl.google.GoogleCloudServiceImpl - Saving table: 
            {"datasetId":"ontologies","projectId":"ecarf-1000","tableId":"dbpedia2_cloudex_processor_1456752400618_1456752468034"}, 
            to cloud storage file: gs://dbpedia-fullrun-3/cloudex-processor-1456752400618_1456752468023_QueryResults_0
         */
        
        /*QueryStats stats = service.saveBigQueryResultsToFile("job_gW11WEy0LnT6ueL2-4NfkpgrNCQ", 
                 "cloudex-processor-1456752400618_1456752468023_QueryResults_0", "swetodblp-local" ,1_200_000);
        
        for(String file: stats.getOutputFiles()) {
            System.out.println(file);
        }*/
        
        List<io.cloudex.framework.cloud.entities.StorageObject> objects = this.service.listCloudStorageObjects("swetodblp-local");
        
        List<String> files = new ArrayList<>();
        
        for(io.cloudex.framework.cloud.entities.StorageObject object: objects) {
            String name = object.getName();
            if(name.startsWith("cloudex-processor-1456752400618_1456752468023_QueryResults_0")) {
                files.add(name);
            }
        }
        
        for(String file: files) {

            System.out.println("Downloading query results file: " + file);
            
            String localFile = FileUtils.TEMP_FOLDER + file;
            
            System.out.println(localFile);
        }
    }

}
