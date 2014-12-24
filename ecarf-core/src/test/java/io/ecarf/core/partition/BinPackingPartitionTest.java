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

import static org.junit.Assert.assertEquals;
import io.ecarf.core.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class BinPackingPartitionTest {
	
	private static Map<Long, Double> SCALE1 = new HashMap<>();
	static {
		SCALE1.put(20L, 2.0D);
		SCALE1.put(25L, 3.0D);
		SCALE1.put(10L, 1.0D);
		SCALE1.put(8L, 1.0D);
		SCALE1.put(5L, 0.5D);
		SCALE1.put(2L, 0.5D);
		SCALE1.put(1L, 0.5D);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link io.ecarf.core.partition.BinPackingPartition#partition()}.
	 */
	@Test
	public void testPartitionLargestGTMax() {
		PartitionFunction function = PartitionFunctionFactory.createBinPacking(this.createItems(), 0.0, 10L);
		List<Partition> bins = function.partition();
		
		for(Partition bin: bins) {
			System.out.println(bin);
			
			for(Item item: bin.getItems()) {
				//System.out.println(item);
				assertEquals(SCALE1.get(item.getWeight()), item.getScale());
			}
		}
		//System.out.println(bins);
		assertEquals(5, bins.size());
	}
	
	@Test
	public void testPartitionEqual() {
		PartitionFunction function = PartitionFunctionFactory.createBinPacking(this.createItems(5L, 10), 0.0, 10L);
		List<Partition> bins = function.partition();
		
		Double point5 = new Double(0.5);
		for(Partition bin: bins) {
			System.out.println(bin);
			
			for(Item item: bin.getItems()) {
				//System.out.println(item);
				assertEquals(point5, item.getScale());
			}
		}
		//System.out.println(bins);
		assertEquals(5, bins.size());
		
		// without a specified maximum
		function = PartitionFunctionFactory.createBinPacking(this.createItems(5L, 10), 0.0, null);
		bins = function.partition();
		
		Double one = new Double(1.0);
		for(Partition bin: bins) {
			System.out.println(bin);
			
			for(Item item: bin.getItems()) {
				//System.out.println(item);
				assertEquals(one, item.getScale());
			}
		}
		//System.out.println(bins);
		assertEquals(10, bins.size());
	}
	
	@Test
	public void testSetNumberOfBins() {
		PartitionFunction function = PartitionFunctionFactory.createBinPacking(this.createItems(), 2);
		List<Partition> bins = function.partition();
		
		for(Partition bin: bins) {
			System.out.println(bin + ", sum: " + bin.sum());
			
			for(Item item: bin.getItems()) {
				System.out.println(item);
				//assertEquals(SCALE1.get(item.getWeight()), item.getScale());
			}
		}
		//System.out.println(bins);
		assertEquals(2, bins.size());
	}
	
	@Test
	public void testItemScale() {
		List<Item> items = Utils.setScale(this.createItems(), 10L);
		for(Item item: items) {
			System.out.println(item);
			assertEquals(SCALE1.get(item.getWeight()), item.getScale());
		}
		
	}
	
	/**
	 * 
	 * @return
	 */
	private List<Item> createItems() {
		
		// expected total bins 5
		List<Item> items = new ArrayList<>();
		
		Item item = new Item("key1", 20L);
		items.add(item);
		
		item = new Item("key2", 25L);
		items.add(item);
		
		item = new Item("key3", 10L);
		items.add(item);
		
		item = new Item("key4", 8L);
		items.add(item);
		
		item = new Item("key5", 5L);
		items.add(item);
		
		item = new Item("key6", 2L);
		items.add(item);
		
		item = new Item("key7", 1L);
		items.add(item);
		
		return items;
	}
	
	private List<Item> createItems(Long weight, Integer count) {
		List<Item> items = new ArrayList<>();
		
		for(int i = 0; i < count; i++) {
			items.add(new Item("key" + i, weight));
		}
		
		return items;
	}

}
