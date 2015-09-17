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
import io.ecarf.core.utils.NumberUtils;
import io.ecarf.core.utils.Utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
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

    public static final int RESOURCE_ID_INVALID = 100;
    public static final String INVALID_RESOURCE = "<invalid>";
    
    /**
     * Resources start at 101, leaving 0 to 99 to be assigned to RDF, RDFS, OWL & OWL2 URIs
     */
    public static final int RESOURCE_ID_START = 101;
     
    private int largestResourceId = RESOURCE_ID_START;
    
    /**
     * The default number of bits used to store information about the parts of an encoded dictionary entry
     * 0, to specify a blank node, 1 to n to specify n number of parts
     */
    private int infoBits = 8;
      
    /**
     * We use a Guava BiMap to provide an inverse lookup into the dictionary Map
     * to be able to lookup terms by id. I'm assuming key lookups in both directions is O(1)
     * @see http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/BiMap.html
     */
    private BiMap<String, Integer> dictionary = HashBiMap.create();
    
    /**
     * Encode a term in the format <http://dbpedia.org/resource/Alexander_II_of_Russia>
     * @param term
     * @return
     */
    public long encode(String term) {
        
        long value = 0;
        
        if(SchemaURIType.RDF_OWL_TERMS.contains(term)) {
            
            value = this.dictionary.get(term);
            
        } else {
            
            // TODO why I'm using this code here instead of TermUtils.split?
            // because I need to efficiently check for https
            String url = term.substring(1, term.length() - 1);
            String path = StringUtils.removeStart(url, TermUtils.HTTP);
            boolean https = false;
            
            if(path.length() == url.length()) {
                path = StringUtils.removeStart(path, TermUtils.HTTPS);
                https = true;
            }
            
            //String [] parts = StringUtils.split(path, URI_SEP);
            // this is alot faster than String.split or StringUtils.split
            List<String> parts = Utils.split(path, TermUtils.URI_SEP);
            
            if(!parts.isEmpty()) {
                
                // 1- encode parts
                long[] values = new long[parts.size()];
                int index = 0;
                Integer enc;
                for(String part: parts) {
                    enc = this.dictionary.get(part);
                    if(enc == null) {
                        throw new IllegalArgumentException("Term part not found in the dictionary: " + part);
                    }
                    
                    values[index] = enc;
                    index++;
                }
                
                // 2- compose
                value = NumberUtils.joinWithInfo(infoBits, values);
                
                // 3- add http/s flag bit
                value <<= 1;
                
                if(https) {
                     value += 1;
                }
            
            } else {
                // invalid URIs, e.g. <http:///www.taotraveller.com> is parsed by NxParser as http:///
                value = RESOURCE_ID_INVALID;
            }
        }
        
        return value;
    }
    
    /**
     * Decode the value provided into a term
     * @param value
     * @return
     */
    public String decode(long value) {
        
        String term = null;
        
        if(value <= RESOURCE_ID_INVALID) {
            
            if(value == RESOURCE_ID_INVALID) {
                term = INVALID_RESOURCE;
                
            } else {
                term = this.dictionary.inverse().get(value);
            }
            
        } else {
            
            // 1- extract http/s flag bit
            String protocol = ((value & 1) == 1) ? TermUtils.HTTPS : TermUtils.HTTP;
            
            // remove the http/s flag
            value >>= 1;
            
            // 2- decompose
            long [] values = NumberUtils.disjoinWithInfo(infoBits, value);
            
            // 3- decode parts
            List<String> parts = new ArrayList<>();
            for(long partv: values) {
                parts.add(this.dictionary.inverse().get(partv));
            }
            
            // 4- add http or https and <>
            term = StringUtils.join(parts, TermUtils.URI_SEP);
            
            term = (new StringBuilder('<')).append(protocol).append(term).append('>').toString();
        }
        
        
        return term;
    }
        
    /**
     * add a part to the dictionary (part of a URI or a blank node)
     * @param part
     */
    public void add(String part) {
        if(!this.dictionary.containsKey(part)) {
            this.largestResourceId++;
            this.dictionary.put(part, this.largestResourceId);
        }
    }
    
    /**
     * Add an entry to the dictionary
     * @param term
     * @param id
     */
    private void add(String term, Integer id) {
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
    
    //-------------------------------------------------------- Binary Serialization 
    /**
     * Serialize this dictionary to a binary file, optionally compressed
     * @param filename
     * @param compress
     * @throws IOException
     */
    public String toFile(String filename, boolean compress) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        Utils.objectToFile(filename, this, compress, false);
                
        log.debug("TIMER# serialized dictionary to file: " + filename + ", in: " + stopwatch);
        
        return filename;
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

        TermDictionary dictionary = Utils.objectFromFile(file, TermDictionary.class, compressed, false);

        log.debug("TIMER# deserialized dictionary from file: " + file + ", in: " + stopwatch);

        return dictionary;

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
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).
                append("largestResourceId", this.largestResourceId).
                append("size", this.dictionary.size()).
                toString();
    }

    /**
     * @return the infoBits
     */
    public int getInfoBits() {
        return infoBits;
    }

    /**
     * @param infoBits the infoBits to set
     */
    public void setInfoBits(int infoBits) {
        this.infoBits = infoBits;
    }

}
