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

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class TermDictionary1 extends AbstractDictionary {

    private static final long serialVersionUID = -3823668914506139740L;
    
    private BidiMap<String, Integer> dictionary = new DualHashBidiMap<>();

    @Override
    public Integer get(String key) {
        
        return this.dictionary.get(key);
    }

    @Override
    public String get(Integer value) {
        
        return this.dictionary.getKey(value);
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

}
