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


package io.ecarf.core.triple;

import io.cloudex.framework.utils.ObjectUtils;
import io.ecarf.core.utils.Utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

/**
 * A stats object for a file containing triple statements. Stats include:
 * 
 * The filename
 * The compressed file size
 * The uncompressed file size
 * The number of statements (triples) in the file
 * The time it took to process the file
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TriplesFileStats implements Serializable {

    private static final long serialVersionUID = -3642336956218003164L;
    
    private String filename;
    
    private Long compressedSize;
    
    private Long size;
    
    private Long statements;
    
    private Long processingTime;

    
    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * @return the compressedSize
     */
    public Long getCompressedSize() {
        return compressedSize;
    }

    /**
     * @param compressedSize the compressedSize to set
     */
    public void setCompressedSize(Long compressedSize) {
        this.compressedSize = compressedSize;
    }

    /**
     * @return the size
     */
    public Long getSize() {
        return size;
    }

    /**
     * @param size the size to set
     */
    public void setSize(Long size) {
        this.size = size;
    }

    /**
     * @return the statements
     */
    public Long getStatements() {
        return statements;
    }

    /**
     * @param statements the statements to set
     */
    public void setStatements(Long statements) {
        this.statements = statements;
    }

    /**
     * @return the processingTime
     */
    public Long getProcessingTime() {
        return processingTime;
    }

    /**
     * @param processingTime the processingTime to set
     */
    public void setProcessingTime(Long processingTime) {
        this.processingTime = processingTime;
    }
    
    //-------------------------------------------------------- JSON Serialization 
    /**
     * Convert to JSON
     * @return json representation of this object.
     */
    public String toJson() {
        return ObjectUtils.GSON.toJson(this);
    }
    
    /**
     * De-serialize a TriplesFileStats instance from a json file.
     * @param jsonFile - a file path
     * @return a TriplesFileStats instance
     * @throws FileNotFoundException if the file is not found
     * @throws IOException if the json conversion fails
     */
    public static List<TriplesFileStats> fromJsonFile(String jsonFile, boolean compressed) throws FileNotFoundException, IOException {
        
        String filename = jsonFile;
        if(compressed) {
            filename = Utils.unCompressFile(jsonFile);
        }
        
        Type token = new TypeToken<List<TriplesFileStats>>(){}.getType();
        
        try(FileReader reader = new FileReader(filename)) {
            List<TriplesFileStats> stats = ObjectUtils.GSON.fromJson(new JsonReader(reader), token);
                   
            return stats;
        }
    }

    /**
     * Serialize this dictionary to a json file, optionally compressed
     * @param filename
     * @param compress
     * @throws IOException
     */
    public static String toJsonFile(String filename, List<TriplesFileStats> stats, boolean compress) throws IOException {
        
        Utils.objectToJsonFile(filename, stats);
        
        if(compress) {
            filename = Utils.compressFile(filename);
        }
                
        return filename;
    }

}
