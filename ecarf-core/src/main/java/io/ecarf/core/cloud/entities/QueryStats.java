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
package io.ecarf.core.cloud.entities;

import java.math.BigInteger;

/**
 * Represent a big data query stats such as number of rows and total bytes processes
 * @author Omer Dawelbeit (omerio)
 *
 */
public class QueryStats {

	private BigInteger totalRows;
	
	private Long totalProcessedBytes;

	public QueryStats() {
		super();
	}

	/**
	 * @param totalRows
	 * @param totalProcessedBytes
	 */
	public QueryStats(BigInteger totalRows, Long totalProcessedBytes) {
		super();
		this.totalRows = totalRows;
		this.totalProcessedBytes = totalProcessedBytes;
	}
	
	/**
	 * @return the totalRows
	 */
	public BigInteger getTotalRows() {
		return totalRows;
	}

	/**
	 * @param totalRows the totalRows to set
	 */
	public void setTotalRows(BigInteger totalRows) {
		this.totalRows = totalRows;
	}

	/**
	 * @return the totalProcessedBytes
	 */
	public Long getTotalProcessedBytes() {
		return totalProcessedBytes;
	}

	/**
	 * @param totalProcessedBytes the totalProcessedBytes to set
	 */
	public void setTotalProcessedBytes(Long totalProcessedBytes) {
		this.totalProcessedBytes = totalProcessedBytes;
	}
	
	
	

}
