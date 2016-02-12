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


package io.ecarf.core.cloud.impl.google;

import io.cloudex.cloud.impl.google.GoogleCloudService;
import io.cloudex.framework.cloud.entities.BigDataTable;
import io.ecarf.core.term.TermCounter;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface EcarfGoogleCloudService extends GoogleCloudService {

    /**
     * Convert the provided file to a format that can be imported to the Cloud Database
     * 
     * @param filename
     * @return
     * @throws IOException 
     */
    public String prepareForBigQueryImport(String filename) throws IOException;
    
    /**
     * Convert the provided file to a format that can be imported to the Cloud Database
     * 
     * @param filename
     * @return
     * @throws IOException 
     */
    public String prepareForBigQueryImport(String filename, final TermCounter counter, boolean countOnly) throws IOException;
    
    /**
     * Stream local N triple files into big query
     * @param files
     * @param table
     * @throws IOException
     */
    public void streamLocalFilesIntoBigData(List<String> files, BigDataTable table) throws IOException;

    /**
     * Download a json file from cloud storage which includes a JSON array and then parse it as a set
     * @param filename
     * @param bucket
     * @return
     * @throws IOException
     */
    public Set<String> getSetFromCloudStorageFile(String filename, String bucket) throws IOException;
    

}
