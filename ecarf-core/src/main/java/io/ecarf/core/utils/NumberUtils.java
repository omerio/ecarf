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

/**
 * 
 * @see http://mathoverflow.net/questions/20646/are-there-any-pairing-functions-computable-in-constant-time-ac%E2%81%B0?newreg=0e4b1ac3e6324bceb746742b6d6adb8e
 * @see http://mathworld.wolfram.com/PairingFunction.html
 * @see https://hbfs.wordpress.com/2011/09/27/pairing-functions/

 * @author Omer Dawelbeit (omerio)
 *
 */
public final class NumberUtils {
    
    /**
     * Sum long values
     * @param values
     * @return the sum of the provided values
     */
    public static long sum(long...values) {
        long sum = 0;
        for(long value: values) {
            sum += value;
        }
        return sum;
            
    }
    
    /**
     * Pair the values provided into one. Use bit interleaving to join them. 
     * @param infobits - the number of bits to reserve for storing the number of values
     * @param values - the values to pair 
     * @return the joined/paired value
     */
    public static long joinWithInfo(int infobits, long... values) {
                
        int max = ((int) Math.pow(2, infobits)) - 1;
        int n = values.length;
        if(max < values.length) {
            throw new IllegalArgumentException(infobits + " is not enough bits to hold the count of " + n + " numbers!");
        }
        
        long p = join(values);
        
        p = (p << infobits) + n;
        
        return p; 
    }
    
    /**
     * Disjoined the provided values into their relevant parts
     * @param infobits - the number of bits used to store the number of values
     * @param value - the value to disjoint
     * @return an array of all the values interleaved in value
     */
    public static long [] disjoinWithInfo(int infobits, long value) {
        
        int max = ((int) Math.pow(2, infobits)) - 1;
        
        int n = (int) value & max;
        
        if(n == 0) {
            throw new IllegalArgumentException("expected one or more values, but found 0");
        }
        
        value >>= infobits;
        
        return disjoin(n, value);
    }
    
    /**
     * Disjoined the provided values into their relevant parts
     * @param n - the number of values to disjoint
     * @param value - the value to disjoint
     * @return an array of all the values interleaved in value
     */
    public static long [] disjoin(int n, long value) {
        long values [] = new long [n];

        if(n == 1) {
            values[0] = value;
            
        } else {
            
            int i = 0;
    
            while (value > 0) {
    
                for(int j = 0; j < n; j++) {
                    values[j] |= (value & 1) << i;
                    value >>= 1;
                }
    
                i++;
            }
        }

        return values;
    }
    
    /**
     * Pair the values provided into one. Use bit interleaving to join them. 
     * @param infobits - the number of bits to reserve for storing the number of values
     * @param values - the values to pair 
     * @return  the joined/paired value
     */
    public static long join(long... values) {
           
        if(values.length == 0) {
            throw new IllegalArgumentException("expecting at least 1 value");
       
        } else if(values.length == 1) {
            return values[0];
        }
        
        long p = 0;
        int i = 0;
        int n = values.length;
        
        while(sum(values) > 0) {
            
            for(int j = 0; j < n; j++) {
                p |= (values[j] & 1) << (i + j);
                values[j] >>= 1;
            }
            
            i += n;
        }
        return p;
    }

}
