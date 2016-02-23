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


package io.ecarf.core.term.dictionary;

import io.ecarf.core.utils.Utils;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Stopwatch;

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
public class TermDictionaryConcurrent extends TermDictionary implements Serializable, ConcurrentDictionary {

    private static final long serialVersionUID = 1314487458787673648L;

    private final static Log log = LogFactory.getLog(TermDictionaryConcurrent.class);


    /**
     * We use a Guava BiMap to provide an inverse lookup into the dictionary Map
     * to be able to lookup terms by id. I'm assuming key lookups in both directions is O(1)
     * @see http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/BiMap.html
     */
    private ConcurrentMap<String, Integer> dictionary = new ConcurrentHashMap<>(1_000_000); //HashBiMap.create();
    
    private final AtomicInteger largestResourceId = new AtomicInteger(RESOURCE_ID_START);


    @Override
    public Integer get(String key) {

        return this.dictionary.get(key);
    }


    @Override
    public String get(Integer value) {

        return null;
    }


    @Override
    public int size() {

        return this.dictionary.size();
    }


    @Override
    public void put(String key, Integer value) {
        this.dictionary.put(key, value);

    }


    /**
     * add a part to the dictionary (part of a URI or a blank node)
     * @param part
     */
    @Override
    public void add(String part) {
        if(!this.dictionary.containsKey(part)) {
            
            int resourceId = this.largestResourceId.incrementAndGet();
            this.dictionary.putIfAbsent(part, resourceId);
        }
    }

    /**
     * Add an entry to the dictionary
     * @param term
     * @param id
     */
    @Override
    protected void add(String term, Integer id) {

        if(!this.dictionary.containsKey(term)) {
            
            int largest = this.largestResourceId.get();

            if(largest < id) {
                this.largestResourceId.set(id);
            }
            this.dictionary.put(term, id);
            
        }
    }


    @Override
    public boolean containsKey(String key) {

        return this.dictionary.containsKey(key);
    }


    /* (non-Javadoc)
     * @see io.ecarf.core.term.AbstractDictionary#putAll(java.util.Map)
     */
    @Override
    protected void putAll(Map<? extends String, ? extends Integer> map) {
        this.dictionary.putAll(map);
    }

    /**
     * @return the largestResourceId
     */
    @Override
    public int getLargestResourceId() {
        return this.largestResourceId.get();
    }
    
    /**
     * Get a non concurrent version of this dictionary
     * @return
     */
    public TermDictionary getNonConcurrentDictionary() {
        log.info("Creating non concurrent dictionary, memory usage: " + Utils.getMemoryUsageInGB());
        Stopwatch stopwatch = Stopwatch.createStarted();
        TermDictionaryCore dict = new TermDictionaryCore();
        dict.setLargestResourceId(this.getLargestResourceId());
        
        /*for(Entry<String, Integer> entry: this.dictionary.entrySet()) {
            dict.put(entry.getKey(), entry.getValue());
        }*/
        dict.putAll(this.dictionary);
        
        log.info("#TIMER finished creating non concurrent dictionary, memory usage: " + Utils.getMemoryUsageInGB() + "GB" + ", timer: " + stopwatch);
        
        return dict;
    }

}
