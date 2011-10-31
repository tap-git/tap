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

    private ColReducer<V, OUT> reducer;
    private AvroCollector<OUT> collector;
    private ReduceIterable reduceIterable = new ReduceIterable();
    private TapContext<OUT> context;
    protected boolean isPipeReducer = false;
    protected OUT out;
    protected OutPipe<OUT> outpipe = null;

    protected abstract ColReducer<V, OUT> getReducer(JobConf conf);

    protected abstract AvroCollector<OUT> getCollector(OutputCollector<KO, VO> c);

    @SuppressWarnings({ "unchecked" })
    @Override
    public void configure(JobConf conf) {
        this.reducer = getReducer(conf);

        try {
            this.out = (OUT) Class.forName(conf.get(Phase.REDUCE_OUT_CLASS)).newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Determine if we are using legacy reduce signature or newer Pipe based signature
        this.isPipeReducer = (null != conf.get(Phase.REDUCER_OUT_PIPE_CLASS));
        if (isPipeReducer) {
            this.outpipe = new OutPipe<OUT>(out);
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

    @Override
    public final void reduce(AvroKey<K> key, Iterator<AvroValue<V>> values,
            OutputCollector<KO, VO> collector, Reporter reporter)
            throws IOException {
        if (this.collector == null) {
            this.collector = getCollector(collector);
        }

        if (this.isPipeReducer) {
            // create an Iterator inPipe
            InPipe<V> inPipe = new InPipe<V>((Iterator<V>)values);
            reducer.reduce(inPipe, this.outpipe);
        } else {
            this.context = new TapContext<OUT>(this.collector, reporter);
            reduceIterable.values = values;
            reducer.reduce(reduceIterable, out, context);
        }
    }

    @Override
    public void close() throws IOException {
        reducer.close(out, context);
    }

}
