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
package io.ecarf.core.cloud.task;

import io.ecarf.core.partition.Item;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

/**
 * 
 * Task Results
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Results {
	
	private final static Logger log = Logger.getLogger(Results.class.getName()); 
	
	/**
	 * Bins for task distribution
	 */
	private List<List<Item>> bins;
	
	/**
	 * A list of items that have a key and weight, could be terms/occurrences or files/sizes
	 */
	private List<Item> items;
	
	/**
	 * VM names
	 */
	private List<String> nodes;
	
	/**
	 * The scale of each item
	 */
	private Map<String, Double> itemScales;
	
	/**
	 * Return a list of comma separated items for each bin
	 * @return
	 */
	public List<String> getBinItems() {
		List<String> binItems = null;
		if(bins.size() > 0) {
			binItems = new ArrayList<>();
			this.itemScales = new HashMap<>();
			
			Long totalWeight = 0L;
			for(List<Item> bin: bins) {
				//System.out.println("Set total: " + bin.size() + ", Set" + bin + ", Sum: " + Utils.sum(bin) + "\n");
				List<String> files = new ArrayList<>();
				Long binWeight = 0L;
				for(Item item: bin) {
					files.add(item.getKey());
					binWeight += item.getWeight();
					
				}
				
				Double scale = (bin.size() == 1) ? bin.get(0).getScale() : 1d;
				
				log.info("Bin weight: " + binWeight + ", bin items: " + bin.size() + ", scale: " + scale);
				
				totalWeight += binWeight;
				
				String itemsStr = StringUtils.join(files, ',');	
				binItems.add(itemsStr);
				this.itemScales.put(itemsStr, scale);
				
			}
			log.info("Total weight of all items: " + totalWeight);
		}
		return binItems;
	}
	
	public Results newNodes() {
		this.nodes = new ArrayList<>();
		return this;
	}


	/**
	 * @return the bins
	 */
	public List<List<Item>> getBins() {
		return bins;
	}


	/**
	 * @param bins the bins to set
	 */
	public Results setBins(List<List<Item>> bins) {
		this.bins = bins;
		return this;
	}


	/**
	 * @return the nodes
	 */
	public List<String> getNodes() {
		return nodes;
	}


	/**
	 * @param nodes the nodes to set
	 */
	public Results setNodes(List<String> nodes) {
		this.nodes = nodes;
		return this;
	}

	/**
	 * @return the items
	 */
	public List<Item> getItems() {
		return items;
	}

	/**
	 * @param items the items to set
	 */
	public Results setItems(List<Item> items) {
		this.items = items;
		return this;
	}
	
	/**
	 * @return the itemScales
	 */
	public Map<String, Double> getItemScales() {
		return itemScales;
	}


}
