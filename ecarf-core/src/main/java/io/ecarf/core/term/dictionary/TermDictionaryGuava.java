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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

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
public class TermDictionaryGuava extends TermDictionary implements Serializable {

    private static final long serialVersionUID = 1314487458787673648L;
    
    //private final static Log log = LogFactory.getLog(TermDictionaryGuava.class);
    
      
    /**
     * We use a Guava BiMap to provide an inverse lookup into the dictionary Map
     * to be able to lookup terms by id. I'm assuming key lookups in both directions is O(1)
     * @see http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/BiMap.html
     */
    private BiMap<String, Integer> dictionary = HashBiMap.create();


    @Override
    public Integer get(String key) {
        
        return this.dictionary.get(key);
    }


    @Override
    public String get(Integer value) {
        
        return this.dictionary.inverse().get(value);
    }


    @Override
    public int size() {
        
        return this.dictionary.size();
    }


    @Override
    public void put(String key, Integer value) {
        this.dictionary.put(key, value);
        
    }


    @Override
    public boolean containsKey(String key) {
       
        return this.dictionary.containsKey(key);
    }


    /**
     * @param map
     * @see com.google.common.collect.BiMap#putAll(java.util.Map)
     */
    @Override
    protected void putAll(Map<? extends String, ? extends Integer> map) {
        this.dictionary.putAll(map);
    }
    
    

}
