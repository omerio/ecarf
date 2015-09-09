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


package io.ecarf.core.term;

import io.cloudex.framework.utils.ObjectUtils;
import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.utils.BiMapJsonDeserializer;
import io.ecarf.core.utils.Utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

/**
 * Represent a term dictionary holding URIs and Blank nodes as the key and the encoded integer as the value
 * Literals are not encoded by this dictionary. To differentiate between Ids for resources vs. blank nodes 
 * we use prime numbers for Blank nodes. {@link BigInteger} primality methods are used to check for ids used
 * by blank nodes. Another approach for blank nodes is that we use a fixed threshold for them, for example
 * from 100 to 1,000,000 are blank nodes, but this puts a superficial limit on the number of blank nodes we 
 * can have. Using prime number has the advantage of adding infinite number of blank nodes.
 *   
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TermDictionary implements Serializable {

    private static final long serialVersionUID = 1314487458787673648L;
    
    private final static Log log = LogFactory.getLog(TermDictionary.class);
    
    private static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(BiMap.class,  new BiMapJsonDeserializer())
            .create(); 
    
    /**
     * Resources start at 100, leaving 0 to 99 to be assigned to RDF, RDFS, OWL & OWL2 URIs
     */
    public static final int RESOURCE_ID_START = 100;
    
    /**
     * The first prime that is larger than 100 to be used for Blank Nodes
     */
    public static final int BLANK_NODE_ID_START = 101;
    
    private int largestResourceId = RESOURCE_ID_START;
    
    private int largestBlankNodeId = BLANK_NODE_ID_START;
    
    /**
     * We use a Guava BiMap to provide an inverse lookup into the dictionary Map
     * to be able to lookup terms by id. I'm assuming key lookups in both directions is O(1)
     * @see http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/BiMap.html
     */
    private BiMap<String, Integer> dictionary = HashBiMap.create();;
    
    /**
     * Encode a resource
     * @param term
     * @return
     */
    public Integer encode(String term) {
        return this.dictionary.get(term);
    }
    
    /**
     * Inverse lookup by Id
     * @param id
     * @return
     */
    public String decode(Integer id) {
        return this.dictionary.inverse().get(id);
    }
    
    /**
     * add a resource to the dictionar
     * @param term
     */
    public void add(String term) {
        this.largestResourceId++;
        this.dictionary.put(term, this.largestResourceId);
    }
    
    /**
     * Add an entry to the dictionary
     * @param term
     * @param id
     */
    public void add(String term, Integer id) {
        if(this.largestResourceId < id) {
            this.largestResourceId = id;
        }
        this.dictionary.put(term, id);
    }
    
    /**
     * Return the number of entries in this dictionary
     * @return
     */
    public int getNumberOfEntries() {
        return this.dictionary.size();
    }
    
    /**
     * Add a blank node after 
     * TODO get next prime
     * @param bnode
     * @param id
     */
    public void addBlankNode(String bnode, Integer id) {
        this.dictionary.put(bnode, id);
    }
    
    /**
     * Create a dictionary pre-populated with the default RDF and OWL URIs
     * @return
     */
    public static TermDictionary createRDFOWLDictionary() {
        TermDictionary dictionary = new TermDictionary();
        for(SchemaURIType uri: SchemaURIType.values()) {
            dictionary.add(uri.uri, uri.id);
        }
        
        return dictionary;
    }
    
    
    /**
     * Convert to JSON
     * @return json representation of this object.
     */
    public String toJson() {
        return ObjectUtils.GSON.toJson(this);
    }
    
    /**
     * De-serialize a TermDictionary instance from a json file.
     * @param jsonFile - a file path
     * @return a TermDictionary instance
     * @throws FileNotFoundException if the file is not found
     * @throws IOException if the json conversion fails
     */
    public static TermDictionary fromJsonFile(String jsonFile) throws FileNotFoundException, IOException {
        return fromJsonFile(jsonFile, false);
    }

    /**
     * De-serialize a TermDictionary instance from a json file.
     * @param jsonFile - a file path
     * @return a TermDictionary instance
     * @throws FileNotFoundException if the file is not found
     * @throws IOException if the json conversion fails
     */
    public static TermDictionary fromJsonFile(String jsonFile, boolean compressed) throws FileNotFoundException, IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        String filename = jsonFile;
        if(compressed) {
            filename = Utils.unCompressFile(jsonFile);
        }
        
        try(FileReader reader = new FileReader(filename)) {
            TermDictionary dictionary = GSON.fromJson(new JsonReader(reader), TermDictionary.class);
            
            log.debug("TIMER# deserialized dictionary from JSON file: " + jsonFile + ", in: " + stopwatch);
            
            return dictionary;
        }
    }
    
    /**
     * De-serialize a TermDictionary instance from a binary file.
     * @param jsonFile - a file path
     * @return a TermDictionary instance
     * @throws FileNotFoundException if the file is not found
     * @throws IOException if the io operation fails
     * @throws ClassNotFoundException 
     */
    public static TermDictionary fromFile(String file, boolean compressed) 
            throws FileNotFoundException, IOException, ClassNotFoundException {
        
        Stopwatch stopwatch = Stopwatch.createStarted();

        String filename = file;
        if(compressed) {
            filename = Utils.unCompressFile(file);
        }

        TermDictionary dictionary = Utils.objectFromFile(filename, TermDictionary.class);

        log.debug("TIMER# deserialized dictionary from JSON file: " + file + ", in: " + stopwatch);

        return dictionary;

    }
    
    /**
     * Serialize this dictionary to a json file, optionally compressed
     * @param filename
     * @param compress
     * @throws IOException
     */
    public String toJsonFile(String filename, boolean compress) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Utils.objectToJsonFile(filename, this);
        
        if(compress) {
            filename = Utils.compressFile(filename);
        }
        
        log.debug("TIMER# serialized dictionary to JSON file: " + filename + ", in: " + stopwatch);
        
        return filename;
    }
    
    /**
     * Serialize this dictionary to a binary file, optionally compressed
     * @param filename
     * @param compress
     * @throws IOException
     */
    public String toFile(String filename, boolean compress) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Utils.objectToFile(filename, this);
        
        if(compress) {
            filename = Utils.compressFile(filename);
        }
        
        log.debug("TIMER# serialized dictionary to JSON file: " + filename + ", in: " + stopwatch);
        
        return filename;
    }

    /**
     * De-serialize a TermDictionary instance from a json string.
     * @param json - a json string
     * @return a TermDictionary instance
     */
    public static TermDictionary fromJsonString(String json) {
        return GSON.fromJson(json, TermDictionary.class);
    }

    /**
     * @return the largestResourceId
     */
    public int getLargestResourceId() {
        return largestResourceId;
    }

    /**
     * @return the largestBlankNodeId
     */
    public int getLargestBlankNodeId() {
        return largestBlankNodeId;
    }

}
