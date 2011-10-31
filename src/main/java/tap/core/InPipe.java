/*
 * Licensed to Think Big Analytics, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Think Big Analytics, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Copyright 2011 Think Big Analytics. All Rights Reserved.
 */
package tap.core;

import java.util.Iterator;

public class InPipe<T> extends Pipe<T>  implements Iterable<T>, Iterator<T> {

    private Iterator<T> values;
    
    public InPipe(T prototype) {
        super(prototype);
    }
    
    /**
     * Reducer In pipe
     * @param values
     */
    public InPipe(Iterator<T> values) {
        super("");
        this.values = values;
    }


    public boolean hasNext() {
        return this.values.hasNext();
    }
    
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();        
    }

    /**
     * Get next object of Type <T> from @Pipe
     * @return Object value
     */
    public T next() {
        return (T) this.values.next();
    }
    
    /**
     * Alias for next()
     * @return The next value in the Iterator
     */
    public T get() {
        return next();
    }

}
