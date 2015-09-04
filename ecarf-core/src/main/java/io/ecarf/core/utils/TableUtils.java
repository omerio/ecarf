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


package io.ecarf.core.utils;

import io.cloudex.cloud.impl.google.compute.GoogleMetaData;
import io.cloudex.framework.cloud.entities.BigDataTable;
import io.ecarf.core.triple.TermType;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public final class TableUtils {
    
    /**
     * Get the {@link BigDataTable} for a BigQuery table
     * @param name
     * @return
     */
    public static BigDataTable getBigQueryTripleTable(String name) {
        
        BigDataTable table = new BigDataTable(name);
        
        for(TermType term: TermType.values()) {
            
            table.addColumn(term.term(), GoogleMetaData.TYPE_STRING);
        }
        
        return table;
        
    }

}
