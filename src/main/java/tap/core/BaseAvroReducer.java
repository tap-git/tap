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
 * Copyright 2010 Think Big Analytics. All Rights Reserved.
 */
package tap.core;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.mapred.*;
import org.apache.hadoop.mapred.*;

import tap.util.ObjectFactory;

/** Base class for a combiner or a reducer */
@SuppressWarnings("deprecation")
abstract class BaseAvroReducer<K, V, OUT, KO, VO> extends MapReduceBase implements Reducer<AvroKey<K>, AvroValue<V>, KO, VO> {

    private TapReducerInterface<V, OUT> reducer;
    private AvroMultiCollector<OUT> collector;
    private ReduceIterable reduceIterable = new ReduceIterable();
    private TapContext<OUT> context;
    protected boolean isPipeReducer = false;
    protected OUT out;
    protected Pipe<OUT> outpipe = null;

    protected abstract TapReducerInterface<V, OUT> getReducer(JobConf conf);

    protected abstract AvroMultiCollector<OUT> getCollector(OutputCollector<KO, VO> c, Reporter reporter);

    @SuppressWarnings({ "unchecked" })
    @Override
    public void configure(JobConf conf) {
        this.reducer = getReducer(conf);

        try {
            this.out = (OUT) ObjectFactory.newInstance(Class.forName(conf.get(Phase.REDUCE_OUT_CLASS)));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Determine if we are using legacy reduce signature or newer Pipe based signature
        isPipeReducer = (null != conf.get(Phase.REDUCER_OUT_PIPE_CLASS));
        if (isPipeReducer) {
            outpipe = new Pipe<OUT>(out);
        }
        if (null != reducer) {
        	reducer.init(conf.get("mapred.output.dir"));
        }
    }

    class ReduceIterable implements Iterable<V>, Iterator<V> {
        private Iterator<AvroValue<V>> values;

        public boolean hasNext() {
            return values.hasNext();
        }

        public V next() {
            return values.next().datum();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public Iterator<V> iterator() {
            return this;
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public final void reduce(AvroKey<K> key, Iterator<AvroValue<V>> values,
            OutputCollector<KO, VO> collector, Reporter reporter)
            throws IOException {
        if (this.collector == null) {
            this.collector = getCollector(collector, reporter);
        }

        if (this.isPipeReducer) {
            // create an Iterator inPipe
            Pipe<V> inPipe = new Pipe<V>((Iterator<AvroValue<V>>)values);
            if (null == this.outpipe.getContext()) {
                this.outpipe.setContext(new TapContext<OUT>(this.collector, reporter));
            }
            
            if(this.collector instanceof BinaryKeyAwareCollector) {
            	byte[] binaryKey = null; // TODO: generate byte representation from Phase.MAP_OUT_KEY_SCHEMA
            	((BinaryKeyAwareCollector) this.collector).setCurrentKey(binaryKey);
            }
            reducer.reduce(inPipe, outpipe);
        }
    }

    @Override
    public void close() throws IOException {
        reducer.close(outpipe);
    }
    
    interface BinaryKeyAwareCollector {
    	void setCurrentKey(byte[] key);
    }

}
