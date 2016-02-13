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

import io.cloudex.framework.cloud.entities.StorageObject;
import io.cloudex.framework.partition.entities.Item;
import io.cloudex.framework.task.CommonTask;
import io.ecarf.core.triple.TriplesFileStats;
import io.ecarf.core.utils.Constants;
import io.ecarf.core.utils.FilenameUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Read a list of files from cloud storage and based on their size split them
 * using a bin packing algorithm
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class CreateFileItemsTask extends CommonTask {
	
	private final static Log log = LogFactory.getLog(CreateFileItemsTask.class);
	
	private String bucket;
	
	// add files matching this string, we use indexOf
	private String fileMatch;
	
	// we only want part of the input if this value is provided, 0 means no max
	private long maxSize;
	

	/* (non-Javadoc)
	 * @see io.ecarf.core.cloud.task.CommonTask#run()
	 */
	@Override
	public void run() throws IOException {
		
		log.info("Processing partition load, with maxSize: " + maxSize);
		
		//String bucket = this.input.getBucket();
		
		List<StorageObject> objects = this.getCloudService().listCloudStorageObjects(bucket);
		
		List<Item> items = new ArrayList<>();
		
		String triplesFilesStats = null;
		
		long totalSize = 0;
		
		for(StorageObject object: objects) {
		    
			String filename = object.getName();
			
			boolean addFile = false;
			
			if(StringUtils.isBlank(fileMatch)) {
			    // fall back into NTriples compressed
			    addFile = filename.endsWith(Constants.COMPRESSED_N_TRIPLES);
			    
			} else {
			    
			    addFile = filename.indexOf(fileMatch) > -1;
			}
			
			if(addFile) {
			    
			    long size = object.getSize().longValue();
			    totalSize = totalSize + size;
				items.add(new Item(filename, size));
				
				if(maxSize > 0 && totalSize >= maxSize) {
				    log.info("Got files with the maximum size of: " + totalSize);
				    break;
				}
				
			} else if(FilenameUtils.TRIPLES_FILES_STATS_JSON.equals(filename)) {
			    
			    triplesFilesStats = filename;
			    log.info("Found triples files stats: " + filename);
			    
			} else {
				log.warn("Skipping file: " + filename);
			}
			
			
		}
		
		// if the triples files stats is found then use it
		if(triplesFilesStats != null) {
		    
		    totalSize = 0;
		    
		    Map<String, Item> fileItems = new HashMap<>();
		    // quickly key items by filename
		    for(Item item: items) {
		        fileItems.put(item.getKey(), item);
		    }
		    
		    try {
		        
		        String localFile = FilenameUtils.getLocalFilePath(triplesFilesStats);
		        this.cloudService.downloadObjectFromCloudStorage(triplesFilesStats, localFile, bucket);

		        List<TriplesFileStats> stats = TriplesFileStats.fromJsonFile(localFile, false);
		        
		        for(TriplesFileStats stat: stats) {
		            Item item = fileItems.get(stat.getFilename());
		            
		            if(item != null)  {
		                
		                long size = 0;
		                
		                if(stat.getProcessingTime() != null) {
		                    
		                    size = stat.getProcessingTime().longValue();     
		                    item.setWeight(size);
		                
		                } else if(stat.getStatements() != null) {
		                    
		                    size = stat.getStatements();
		                    item.setWeight(size);
		                
		                } else if(stat.getSize() != null) {
		                    
		                    size = stat.getSize();
		                    item.setWeight(size);
		                }
		                
		                totalSize = totalSize + size;
		            }
		        }

		    } catch(Exception e) {

		        log.error("Failed to download and process triples files stats", e);
		    }
		}
		
		// add the items to the output
		this.addOutput("fileItems", items);
		
		
		// each node should handle a gigbyte of data
		// read it the configurations
		/*PartitionFunction function;
		
		if(this.input.getNumberOfNodes() == null) {
			function = PartitionFunctionFactory.createBinPacking(items, 
					this.input.getNewBinPercentage(), 
					this.input.getWeightPerNode());
		} else {
			function = PartitionFunctionFactory.createBinPacking(items, this.input.getNumberOfNodes());
		}
		
		List<Partition> bins = function.partition();
		
		this.results = new Results();
		results.setBins(bins);*/
		
		log.info("Successfully processed partition load, total items: " + items.size() + ", total size: " + totalSize);
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
     * @return the fileMatch
     */
    public String getFileMatch() {
        return fileMatch;
    }


    /**
     * @param fileMatch the fileMatch to set
     */
    public void setFileMatch(String fileMatch) {
        this.fileMatch = fileMatch;
    }


    /**
     * @return the maxSize
     */
    public long getMaxSize() {
        return maxSize;
    }


    /**
     * @param maxSize the maxSize to set
     */
    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }
	

}
