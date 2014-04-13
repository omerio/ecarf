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
import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class SetPartition {
	
	public static double excess_factor = 0.10;
	
	/**
	 * 
	 * @param set
	 * @return
	 */
	public static int sum(List<Integer> set) {
		int sum = 0;
		for(Integer item: set) {
			sum += item;
		}
		return sum;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		List<Integer> items = Lists.newArrayList(100, 35, 20, 5, 6, 7, 9, 75, 85, 60);
		
		Collections.sort(items,Collections.reverseOrder());
		
		int max = items.get(0);
		System.out.println("Maximum is: " + max);
		
		System.out.println("New set if sum of items larger than: " + (max * excess_factor));
		
		int sum = sum(items);
		
		System.out.println("Total sum is: " + sum);
		
		double numSets = (sum / (float) max);
		double numSetsInt = Math.floor(numSets);
		int finalNumSets = (int) numSetsInt;
		double excess = numSets - numSetsInt;
		if(excess > excess_factor) {
			finalNumSets++;
		}
		
		System.out.println("Number of required sets: " + (sum / (float) max));
		System.out.println("Number of required sets: " + finalNumSets);
		
		// 
		List<List<Integer>> sets = new ArrayList<>();
		for(int i = 0; i < finalNumSets; i++) {
			List<Integer> set = new ArrayList<Integer>();
			sets.add(set);
			Integer cMax = items.get(0);
			set.add(cMax);
			items.remove(cMax);
			
			Integer sSum = sum(set);
			
			if(sSum < max) {
				Integer diff = max - sSum;
				for(int j = 0; j < items.size(); j++) {
					Integer element = items.get(j);
					if(element <= diff) {	
						
						set.add(element);
						items.remove(j);
						
						if(element == diff) {
							break;
						} else {
							// look for an even small number to fill the gap
							diff = max - sum(set);
						}
					} 
				}
			}
		}
		
		//add anything that is remaining to the last set
		if(!items.isEmpty()) {
			sets.get(sets.size() - 1).addAll(items);
			items.clear();
		}
		
		System.out.println(sets);
		
		for(List<Integer> set: sets) {
			System.out.println("Set: " + set + ", Sum: " + sum(set));
		}
		
		//List<Integer> items = Lists.newArrayList(100, 35, 20, 5, 6, 1, 7, 9, 75, 85, 60);
		items = Lists.newArrayList(100, 35, 20, 5, 6, 7, 9, 75, 85, 60);
		List<Item> cItems = new ArrayList<>();
		
		for(Integer item: items) {
			cItems.add(new Item("Key" + (new Date()).getTime(), new Long(item)));
		}
		
		PartitionFunction function = PartitionFunctionFactory.createBinPacking(cItems, 0.1, null);
		List<List<Item>> bins = function.partition();
		
		//System.out.println(bins);

		for(List<Item> bin: bins) {
			System.out.println("Set: " + bin + ", Sum: " + Utils.sum(bin) + "\n");
		}
		
			
	}

}
