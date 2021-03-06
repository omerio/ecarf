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

import io.cloudex.cloud.impl.google.bigquery.BigQueryStreamable;

/**
 * @author Omer Dawelbeit (omerio)
 *
 */
public interface Triple extends BigQueryStreamable {

    public Object getSubject();
    
    public Object getPredicate();
    
    public Object getObject();
    
    public String getObjectLiteral();
    
    public void setObjectLiteral(String literal);
    
    public boolean isInferred();
    
    public void setInferred(boolean inferred);
    
    public boolean isEncoded();
    
    public Triple create(Object subject, Object predicate, Object object);

    public String toCsv();

    public void set(String string, Object value);

}
