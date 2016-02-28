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
package io.ecarf.core.cloud.task.processor.reason.phase2;

import io.cloudex.framework.cloud.entities.QueryStats;
import io.ecarf.core.utils.Utils;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class QueryResult {

	private QueryStats stats;
	
	private String filename;
		
	private String jobId;
	
	/**
	 * @return the stats
	 */
	public QueryStats getStats() {
		return stats;
	}

	/**
	 * @param stats the stats to set
	 */
	public QueryResult setStats(QueryStats stats) {
		this.stats = stats;
		return this;
	}

	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public QueryResult setFilename(String filename) {
		this.filename = filename;
		return this;
	}

	/**
	 * @return the jobId
	 */
	public String getJobId() {
		return jobId;
	}

	/**
	 * @param jobId the jobId to set
	 */
	public QueryResult setJobId(String jobId) {
		this.jobId = jobId;
		return this;
	}
	
	public static QueryResult create() {
		return new QueryResult();
	}

    /**
     * @return the localFilename
     */
    public String getLocalFilename() {
        return Utils.TEMP_FOLDER + filename;
    }

    /**
     * Delegate for stats.getTotalRows
     * @return
     */
    public long getTotalRows() {
        long rows = 0;
        
        if((this.stats != null) && (this.stats.getTotalRows() != null)) {
            rows = this.stats.getTotalRows().longValue();
        }
        
        return rows;
    }

}
