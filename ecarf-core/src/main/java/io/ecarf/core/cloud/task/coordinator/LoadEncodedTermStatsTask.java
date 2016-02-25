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

import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.task.CommonTask;
import io.cloudex.framework.utils.FileUtils;
import io.ecarf.core.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Load the encoded terms stats and set them on the job context
 * @author Omer Dawelbeit (omerio)
 *
 */
public class LoadEncodedTermStatsTask extends CommonTask {

    private final static Log log = LogFactory.getLog(LoadEncodedTermStatsTask.class);

    private String bucket;
    
    private String encodedTermStatsFile;


    /* (non-Javadoc)
     * @see io.cloudex.framework.Executable#run()
     */
    @Override
    public void run() throws IOException {

        log.info("Loading encoded term stats map from: " + this.encodedTermStatsFile);
        
        String localFile = Utils.TEMP_FOLDER + encodedTermStatsFile;
        
        this.cloudService.downloadObjectFromCloudStorage(this.encodedTermStatsFile, localFile, this.bucket);

        // convert from JSON
        Map<String, Long> termStats = FileUtils.jsonFileToMap(localFile);

        if(!termStats.isEmpty()) {

            List<Item> items = new ArrayList<>();
            for(Entry<String, Long> item: termStats.entrySet()) {
                Item anItem = (new Item()).setKey(item.getKey()).setWeight(item.getValue());
                items.add(anItem);
            }

            this.addOutput("termItems", items);

            log.info("Successfully added term stats to the job context: " + items);

        }

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
     * @return the encodedTermStatsFile
     */
    public String getEncodedTermStatsFile() {
        return encodedTermStatsFile;
    }


    /**
     * @param encodedTermStatsFile the encodedTermStatsFile to set
     */
    public void setEncodedTermStatsFile(String encodedTermStatsFile) {
        this.encodedTermStatsFile = encodedTermStatsFile;
    }

}
