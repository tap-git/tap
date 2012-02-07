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

import org.apache.avro.mapred.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;

import tap.core.io.BinaryKey;
import tap.core.mapreduce.io.ProtobufWritable;
import tap.core.mapreduce.output.TapfileOutputFormat;

/**
 * Bridge between a {@link org.apache.hadoop.mapred.Reducer} and an {@link AvroReducer}.
 */
class ReducerBridge<V, OUT> extends BaseAvroReducer<V, OUT, AvroWrapper<OUT>, NullWritable> {
    
    private boolean isTextOutput = false;
    private boolean isProtoOutput = false;
    
    private String multiOutputPrefix;
    private MultipleOutputs multiOutput;
    

    @Override
    public void configure(JobConf conf) {
        super.configure(conf);
        isTextOutput = conf.getOutputFormat() instanceof TextOutputFormat;
        isProtoOutput = conf.getOutputFormat() instanceof TapfileOutputFormat;
       
        /*
        System.out.println(conf.getOutputFormat().getClass());
        System.out.println(conf.getOutputKeyClass());
        System.out.println(conf.getOutputValueClass());
        */
        
        multiOutputPrefix = conf.get(Phase.MULTIPLE_OUTPUT_PREFIX);
        if(multiOutputPrefix == null)
            multiOutputPrefix = "out";
        
        MultipleOutputs.addMultiNamedOutput(
                conf, multiOutputPrefix, conf.getOutputFormat().getClass(),
                conf.getOutputKeyClass(), conf.getOutputValueClass());
        
        this.multiOutput = new MultipleOutputs(conf);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected TapReducerInterface<V, OUT> getReducer(JobConf conf) {
    	Class<? extends TapReducerInterface> theClass = conf.getClass(Phase.REDUCER, TapReducer.class, TapReducerInterface.class);
        return ReflectionUtils.newInstance(theClass, conf);
    }

    private class ReduceCollector<AO, OUT> extends AvroMultiCollector<AO> implements BinaryKeyAwareCollector {
        private final AvroWrapper<OUT> wrapper = new AvroWrapper<OUT>(null);
        private Reporter reporter;
        private OutputCollector originalCollector;
        private ProtobufWritable protobufWritable = new ProtobufWritable();
        private BinaryKey binaryKey;

        public ReduceCollector(OutputCollector<?, NullWritable> out, Reporter reporter) {
            this.originalCollector = out;
            this.reporter = reporter;
        }

        @SuppressWarnings("unchecked")
        private void _collect(Object datum, OutputCollector out) throws IOException {
            if (isTextOutput) {
                out.collect(datum, NullWritable.get());
            } else if(isProtoOutput) {
                if(datum != null)
                    protobufWritable.setConverter(datum.getClass());
                protobufWritable.set(datum);
                // TODO: out.collect(binaryKey, protobufWritable);
                out.collect(NullWritable.get(), protobufWritable);
            }
            else {
                wrapper.datum((OUT) datum);
                out.collect(wrapper, NullWritable.get());
            }
        }
        
        @Override
        public void collect(Object datum) throws IOException {
           _collect(datum, originalCollector); 
        }
        
        @Override
        public void collect(Object datum, String multiName) throws IOException {
            OutputCollector collector = multiOutput.getCollector(multiOutputPrefix, multiName, reporter);
            _collect(datum, collector);
        }

		@Override
		public void setCurrentKey(BinaryKey key) {
			binaryKey = key;
		}
    }

    @Override
    protected AvroMultiCollector<OUT> getCollector(OutputCollector<AvroWrapper<OUT>, NullWritable> collector, Reporter reporter) {
        return new ReduceCollector(collector, reporter);
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        multiOutput.close();
    }

}

