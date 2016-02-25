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

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * One place for all the filenames used by ECARF.
 * Avoids the mess of the various filenames being composed all over the code
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public final class FilenameUtils {
    
    public static final String TRIPLES_FILES_STATS_JSON = "triples_files_stats.json";
    
    public static final String KRYO_SERIALIZED_EXT = ".kryo.gz";
    
    public static final String DICTIONARY = "dictionary";
        
    public static final String DICTIONARY_JSON = DICTIONARY + Constants.DOT_JSON;
    
    public static final String DICTIONARY_SER = DICTIONARY + Constants.DOT_SER;
    
    /**
     * Return the path to local file that lives in the temp folder
     * 
     * @param filename
     * @return
     */
    public static String getLocalFilePath(String filename) {
        return Utils.TEMP_FOLDER + filename;
    }
    
    /**
     * Check if a file exists
     * @param filename
     * @return
     */
    public static boolean fileExists(String filename) {
        return Files.exists(Paths.get(filename));

    }
    
    /**
     * The serialized and gziped dictionary filename
     * @return
     */
    public static String getSerializedGZipedDictionaryFilename() {
        return DICTIONARY + KRYO_SERIALIZED_EXT;
    }

    /**
     * Return the path to local file that is serialized (java or Kryo) and is compressed
     * @param filename
     * @param java
     * @return
     */
    public static String getLocalSerializedGZipedFilePath(String filename, boolean java) {
        return getLocalFilePath(filename) + 
                (java ? Constants.DOT_SER : Constants.DOT_KRYO) + 
                Constants.GZIP_EXT;
    }
    
    /**
     * Return the path to the blank nodes local file that is serialized (java or Kryo) and is compressed
     * @param filename
     * @param java
     * @return
     */
    public static String getLocalSerializedGZipedBNFilePath(String filename, boolean java) {
        return getLocalSerializedGZipedFilePath(Constants.BLANK_NODES + filename, java);
    }
}
