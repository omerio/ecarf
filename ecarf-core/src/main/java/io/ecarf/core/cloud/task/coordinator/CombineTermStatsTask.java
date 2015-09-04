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


package io.ecarf.core.cloud.task.coordinator;

import io.cloudex.cloud.impl.google.compute.GoogleMetaData;
import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class CombineTermStatsTask extends CommonTask {

    private final static Log log = LogFactory.getLog(CombineTermStatsTask.class);

    private String bucket;

    //private String schemaTermsFile;

    private Collection<String> processors;


    /* (non-Javadoc)
     * @see io.cloudex.framework.Executable#run()
     */
    @Override
    public void run() throws IOException {

        // all done, now get the results from cloud storage and combine the schema terms stats
        //if(StringUtils.isNotBlank(this.input.getSchemaTermsFile())) {

        Map<String, Long> allTermStats = new HashMap<String, Long>();

        for(String instanceId: this.processors) {   //this.results.getNodes()) {   
            String statsFile = instanceId + Constants.DOT_JSON;

            String localStatsFile = Utils.TEMP_FOLDER + statsFile;

            try {
                this.getCloudService().downloadObjectFromCloudStorage(statsFile, localStatsFile, bucket);

                // convert from JSON
                Map<String, Long> termStats = FileUtils.jsonFileToMap(localStatsFile);

                for(Entry<String, Long> term: termStats.entrySet()) {
                    String key = term.getKey();
                    Long value = term.getValue();

                    if(allTermStats.containsKey(key)) {
                        value = allTermStats.get(key) + value;
                    } 

                    allTermStats.put(key, value);
                }

                log.info("Evms analysed: " + allTermStats.size() + ", terms");

            } catch(IOException e) {
                // a file not found means the evm didn't find any schema terms so didn't generate any stats
                log.error("failed to download file: " + localStatsFile, e);
                if(!(e.getMessage().indexOf(GoogleMetaData.NOT_FOUND) >= 0)) {
                    throw e;
                }
            }

        }

        if(!allTermStats.isEmpty()) {

            List<Item> items = new ArrayList<>();
            for(Entry<String, Long> item: allTermStats.entrySet()) {
                Item anItem = (new Item()).setKey(item.getKey()).setWeight(item.getValue());
                items.add(anItem);
            }

            //this.results.setItems(items);

            this.addOutput("termItems", items);

            log.info("Successfully created term stats: " + items);

        }
        // }

    }

}
