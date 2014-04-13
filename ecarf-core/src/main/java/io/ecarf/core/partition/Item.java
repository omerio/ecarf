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

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Item implements Comparable<Item>	{

	private String key;

	private Long weight;

	/**
	 * 
	 */
	public Item() {
		super();
	}

	/**
	 * @param key
	 * @param weight
	 */
	public Item(String key, Long weight) {
		super();
		this.key = key;
		this.weight = weight;
	}

	/**
	 * @return the key
	 */
	public String getKey() {
		return key;
	}

	/**
	 * @param key the key to set
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * @return the weight
	 */
	public Long getWeight() {
		return weight;
	}

	/**
	 * @param weight the weight to set
	 */
	public void setWeight(Long weight) {
		this.weight = weight;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return new ToStringBuilder(this).
				append("key", key).
				append("weight", weight).
				toString();
	}

	@Override
	public int compareTo(Item o) {
		return this.weight.compareTo(o.weight);
	}


}
