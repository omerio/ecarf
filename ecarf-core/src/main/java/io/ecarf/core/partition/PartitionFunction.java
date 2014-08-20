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

import java.util.List;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface PartitionFunction {
	
	/**
	 * Add an item to partition
	 * @param item
	 */
	public void addItem(Item item);
	
	/**
	 * Add items to partition
	 * @param items
	 */
	public void addItems(List<Item> items);
	
	/**
	 * partition the items based on the partitioning function used
	 * @return
	 */
	public List<Partition> partition();
	
}
