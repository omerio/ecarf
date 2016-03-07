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
import io.cloudex.framework.cloud.api.ApiUtils;
import io.cloudex.framework.cloud.entities.BigDataTable;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.compress.NxGzipCallback;
import io.ecarf.core.compress.NxGzipProcessor;
import io.ecarf.core.compress.callback.StringEscapeCallback;
import io.ecarf.core.compress.callback.TermCounterCallback;
import io.ecarf.core.term.TermCounter;
import io.ecarf.core.triple.TripleUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableList.Tables;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class EcarfGoogleCloudServiceImpl extends GoogleCloudServiceImpl implements EcarfGoogleCloudService {
    
    private final static Log log = LogFactory.getLog(EcarfGoogleCloudServiceImpl.class);


    /**
     * Convert the provided file to a format that can be imported to the Cloud Database
     * 
     * @param filename
     * @return
     * @throws IOException 
     */
    @Override
    public String prepareForBigQueryImport(String filename) throws IOException {
        return this.prepareForBigQueryImport(filename, null, false);
    }

    /**
     * Convert the provided file to a format that can be imported to the Cloud Database
     * 
     * @param filename
     * @return
     * @throws IOException 
     */
    @Override
    public String prepareForBigQueryImport(String filename, final TermCounter counter, boolean countOnly) throws IOException {
        /*String outFilename = new StringBuilder(FileUtils.TEMP_FOLDER)
              .append(File.separator).append("out_").append(filename).toString();*/
        NxGzipProcessor processor = new NxGzipProcessor(filename);

        NxGzipCallback callback;

        if(countOnly) {
            callback = new TermCounterCallback();
            
        } else {
            
            callback = new StringEscapeCallback();
        }

        callback.setCounter(counter);

        String outFilename = null;
        
        if(countOnly) {
            
            processor.read(callback);
            
        } else {
            outFilename = processor.process(callback);
        }

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
    
    /**
     * Delete BigQuery tables that match the provided string
     * @param datasetId
     * @param match
     * @throws IOException
     */
    public void deleteTables(String datasetId, String match) throws IOException {

       /* Datasets.List datasetRequest = bigquery.datasets().list(projectId);
        DatasetList datasetList = datasetRequest.execute();
        if (datasetList.getDatasets() != null) {
            List<DatasetList.Datasets> datasets = datasetList.getDatasets();
            System.out.println("Available datasets\n----------------");
            System.out.println(datasets.toString());
            for (DatasetList.Datasets dataset : datasets) {
                System.out.format("%s\n", dataset.getDatasetReference().getDatasetId());
            }
        }*/
        
        Bigquery bigquery = this.getBigquery();
        TableList tables = bigquery.tables().list(this.getProjectId(), datasetId)
                .setOauthToken(this.getOAuthToken()).execute();
        
        if(tables.getTables() != null) {
            for(Tables table: tables.getTables()) {
                String tableId = table.getTableReference().getTableId();
                
                if(tableId.contains(match)) {
                    log.info("Deleting table: " + tableId);
                    bigquery.tables().delete(this.getProjectId(), datasetId, tableId).setKey(table.getId()).setOauthToken(this.getOAuthToken()).execute();
                    ApiUtils.block(this.getApiRecheckDelay());
                }
            }
        }

    }
    
    /**
     * Display all BigQuery datasets associated with a project.
     *
     * @param bigquery  an authorized BigQuery client
     * @param projectId a string containing the current project ID
     * @throws IOException Thrown if there is a network error connecting to
     *                     Bigquery.
     */
   /* public static void listDatasets(final Bigquery bigquery, final String projectId)
        throws IOException {
      Datasets.List datasetRequest = bigquery.datasets().list(projectId);
      DatasetList datasetList = datasetRequest.execute();
      if (datasetList.getDatasets() != null) {
        List<DatasetList.Datasets> datasets = datasetList.getDatasets();
        System.out.println("Available datasets\n----------------");
        System.out.println(datasets.toString());
        for (DatasetList.Datasets dataset : datasets) {
          System.out.format("%s\n", dataset.getDatasetReference().getDatasetId());
        }
      }
    }
*/
    /*public void listTableData(final String datasetId,
            final String tableId, String file) throws IOException {
        
        // this returns the first 100,000 table rows in CSV format
        try(OutputStream stream = new BufferedOutputStream(new FileOutputStream(file), Constants.GZIP_BUF_SIZE)) {
                        //new GZIPOutputStream(new FileOutputStream(file), Constants.GZIP_BUF_SIZE))) {
            Bigquery bigquery = this.getBigquery();
            bigquery.tabledata().list(this.getProjectId(), datasetId, tableId).setAlt("CSV")
            .setOauthToken(this.getOAuthToken())
            .executeAndDownloadTo(stream);

            //stream.flush();
        }
        
        
    }*/

  

}
