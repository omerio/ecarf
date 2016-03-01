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


package io.ecarf.core.cloud.task.processor.reason.phase3;

import io.ecarf.core.triple.SchemaURIType;
import io.ecarf.core.triple.Triple;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public class DuplicatesBuster {
    
    private static final Long RDF_TYPE = (long) SchemaURIType.RDF_TYPE.id;
    
    // hashmap keyed by the object of rdf:type vs Set of subjects
    private ConcurrentHashMap<Long, Set<Long>> rdfTypeMap = new ConcurrentHashMap<>();
    
    // hashmap keyed by the predicate with a subject, object tuple
    //private ConcurrentHashMap<Long, Set<Tuple>> rdfSubPropertyMap = new ConcurrentHashMap<>();
    
    public boolean isDuplicate(Triple triple) {
        boolean duplicate = false;
        // ?x, rdf:type, ?c
        
        Long predicate = (Long) triple.getPredicate();
        Long subject = (Long) triple.getSubject();
        Long object = (Long) triple.getObject();
        
        if(RDF_TYPE.equals(predicate)) {

            if(rdfTypeMap.containsKey(object)) {
                
                Set<Long> subjects = rdfTypeMap.get(object);
                if(subjects.contains(subject)) {
                    duplicate = true;
                    
                } else {
                    subjects.add(subject);
                }
                
            } else {
                Set<Long> subjects = new HashSet<>(100000);
                subjects.add(subject);
                rdfTypeMap.putIfAbsent(object, subjects);
            }
            
        } 
        
        // Our source of duplicates is well and truley triples with rdf:type
        
        /*else {
            
            Tuple tuple = new Tuple(subject, object);
            
            if(rdfSubPropertyMap.containsKey(predicate)) {
                
                Set<Tuple> subjectObjects = rdfSubPropertyMap.get(predicate);
                
                if(subjectObjects.contains(tuple)) {
                    duplicate = true;
                    
                } else {
                    subjectObjects.add(tuple);
                }
                
            } else {
                
                Set<Tuple> subjectObjects = new HashSet<>();
                subjectObjects.add(tuple);
                rdfSubPropertyMap.putIfAbsent(object, subjectObjects);
            }
            
        }*/
        
        return duplicate;
    }
    
    
    /**
     * using primitives, although there is unboxing hit, we should save memory
     * @author Omer Dawelbeit (omerio)
     *
     */
   /* public class Tuple { 
        private long subject; 
        private long object;
        
        *//**
         * @param subject
         * @param object
         *//*
        public Tuple(long subject, long object) {
            super();
            this.subject = subject;
            this.object = object;
        }

         (non-Javadoc)
         * @see java.lang.Object#hashCode()
         
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (object ^ (object >>> 32));
            result = prime * result + (int) (subject ^ (subject >>> 32));
            return result;
        }

         (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof Tuple))
                return false;
            Tuple other = (Tuple) obj;
            if (object != other.object)
                return false;
            if (subject != other.subject)
                return false;
            return true;
        }
    
    }
    */
 
}
