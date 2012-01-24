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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Bridge between a {@link org.apache.hadoop.mapred.Reducer} and an {@link AvroReducer} used when combining. When combining, map
 * output pairs must be split before they're collected.
 */
class CombinerBridge<K, V> extends BaseAvroReducer<K, V, V, AvroKey<K>, AvroValue<V>> {

    private Schema schema;
    private String groupBy;
    private String sortBy;

    @Override
    @SuppressWarnings("unchecked")
    protected TapReducerInterface<V, V> getReducer(JobConf conf) {
        return ReflectionUtils.newInstance(conf.getClass(Phase.COMBINER, TapReducer.class, TapReducerInterface.class), conf);
    }

    @Override
    public void configure(JobConf conf) {
        super.configure(conf);
        this.schema = Phase.getSchema(this.out);
        this.groupBy = conf.get(Phase.GROUP_BY);
        this.sortBy = conf.get(Phase.SORT_BY);
    }
    
    private class Collector<VC> extends AvroMultiCollector<VC> {
        //private final AvroWrapper<V> wrapper = new AvroWrapper<V>(null);
        private final AvroKey<K> keyWrapper = new AvroKey<K>(null);
        private final AvroValue<VC> valueWrapper = new AvroValue<VC>(null);
        private final KeyExtractor<K,VC> extractor;
        private final K key;
        private OutputCollector<AvroKey<K>, AvroValue<VC>> collector;

        public Collector(OutputCollector<AvroKey<K>, AvroValue<VC>> collector, KeyExtractor<K,VC> extractor) {
            this.collector = collector;
            this.extractor = extractor;
            key = extractor.getProtypeKey();
            keyWrapper.datum(key);
        }

        public void collect(VC datum) throws IOException {
            extractor.setKey(datum, key);
            valueWrapper.datum(datum);
            collector.collect(keyWrapper, valueWrapper);
        }
    }

    @Override
    protected AvroMultiCollector<V> getCollector(OutputCollector<AvroKey<K>, AvroValue<V>> collector, Reporter reporter) {
        KeyExtractor<GenericData.Record, V> extractor = new ReflectionKeyExtractor<V>(schema, groupBy, sortBy);
        //XXX fix this typing: the collector returns GenericData.Record, not K! should be Collector<V>
        return new Collector(collector, extractor);
    }

}
