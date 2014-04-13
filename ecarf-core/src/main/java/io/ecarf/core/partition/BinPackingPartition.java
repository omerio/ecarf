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
package io.ecarf.core.partition;

import io.ecarf.core.utils.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A variation of the Bin Packing algorithm
 * @see http://mathworld.wolfram.com/Bin-PackingProblem.html
 * @author Omer Dawelbeit (omerio)
 *
 */
public class BinPackingPartition implements PartitionFunction {

	private List<Item> items;// = new ArrayList<>();

	private Long maximum;

	private Double newBinPercentage = 0.5;


	/**
	 * 
	 */
	public BinPackingPartition() {
		super();
	}

	/**
	 * @param items
	 */
	public BinPackingPartition(List<Item> items) {
		super();
		this.items = items;
	}

	@Override
	public List<List<Item>> partition() {
		// sort descending
		Collections.sort(items, Collections.reverseOrder());

		Long max = (this.maximum != null) ? this.maximum : items.get(0).getWeight();
		//System.out.println("Maximum is: " + max);

		//System.out.println("New set if sum of items larger than: " + (max * newBinPercentage));

		long sum = Utils.sum(items);

		//System.out.println("Total sum is: " + sum);

		double numBins = (sum / (float) max);
		double numWholeBins = Math.floor(numBins);
		int finalNumBins = (int) numWholeBins;
		double excess = numBins - numWholeBins;
		if(excess > newBinPercentage) {
			finalNumBins++;
		}

		//System.out.println("Number of required sets: " + (sum / (float) max));
		//System.out.println("Number of required sets: " + finalNumBins);

		// 
		List<List<Item>> bins = new ArrayList<>();
		for(int i = 0; i < finalNumBins; i++) {
			List<Item> bin = new ArrayList<Item>();
			bins.add(bin);
			Item largestItem = items.get(0);
			//Long currentMax = largestItem.getWeight();
			bin.add(largestItem);
			items.remove(largestItem);

			Long currentSum = Utils.sum(bin);

			if(currentSum < max) {
				Long diff = max - currentSum;
				for(int j = 0; j < items.size(); j++) {
					Item item = items.get(j);
					if(item.getWeight() <= diff) {	

						bin.add(item);
						items.remove(j);

						if(item.getWeight() == diff) {
							break;
						} else {
							// look for an even small number to fill the gap
							diff = max - Utils.sum(bin);
						}
					} 
				}
			}
		}

		//add anything that is remaining to the last set
		if(!items.isEmpty()) {
			bins.get(bins.size() - 1).addAll(items);
			items.clear();
		}

		/*System.out.println(bins);

		for(List<Item> bin: bins) {
			System.out.println("Set: " + bin + ", Sum: " + Utils.sum(bin));
		}*/

		return bins;

	}


	@Override
	public void addItem(Item item) {
		this.items.add(item);
	}

	@Override
	public void addItems(List<Item> items) {
		this.items.addAll(items);

	}

	/**
	 * @param maximum the maximum to set
	 */
	public void setMaximum(Long maximum) {
		this.maximum = maximum;
	}

	/**
	 * @param newBinPercentage the newBinPercentage to set
	 */
	public void setNewBinPercentage(Double newBinPercentage) {
		this.newBinPercentage = newBinPercentage;
	}

}
