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

import java.lang.reflect.Type;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/**
 * De-serialize a Google Guava BiMap
 * @see https://gist.github.com/gythialy/75e13e8d4594118809a3
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class BiMapJsonDeserializer implements JsonDeserializer<BiMap<String, Integer>> {

    @Override
    public BiMap<String, Integer> deserialize(JsonElement json,
                             Type type,
                             JsonDeserializationContext context) throws JsonParseException {
        
        BiMap<String, Integer> mapping = HashBiMap.create();
        //Type[] typeParameters = ((ParameterizedType) type).getActualTypeArguments();
        JsonObject object = (JsonObject) json;
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            int value = entry.getValue().getAsInt();
            mapping.put(entry.getKey(), value);
                        //Castors.me()
                          //     .castTo(value,
                                      // (Class) typeParameters[1]));
        }
        return mapping;
    }
    
}
