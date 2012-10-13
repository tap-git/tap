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
package tap.formats.avro;

import java.io.*;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.mapred.*;
import org.apache.avro.protobuf.ProtobufDatumReader;
import org.apache.avro.protobuf.ProtobufDatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.*;

import com.google.protobuf.Message;

import tap.Phase;
import tap.core.io.BinaryKey;
import tap.core.io.avro.BinaryKeyDatumReader;
import tap.core.io.avro.BinaryKeyDatumWriter;
import tap.core.io.avro.BinaryKeyEncoder;

/**
 * Avro serialization format used by Tap. Derived from the Avro map/reduce framework format.
 * The {@link Serialization} used by jobs configured with {@link Phase}. 
 */
public class TapAvroSerialization<T> extends Configured implements Serialization<AvroWrapper<T>> {

    public boolean accept(Class<?> c) {
        return AvroWrapper.class.isAssignableFrom(c);
    }

    /**
     * Returns the specified map output deserializer. Defaults to the final output deserializer if no map output schema was
     * specified.
     */
    @SuppressWarnings("rawtypes")
	public Deserializer<AvroWrapper<T>> getDeserializer(Class<AvroWrapper<T>> c) {
        // We need not rely on mapred.task.is.map here to determine whether map
        // output or final output is desired, since the mapreduce framework never
        // creates a deserializer for final output, only for map output.
        boolean isKey = AvroKey.class.isAssignableFrom(c);
        Schema schema = Schema.parse(isKey ? getConf().get(Phase.MAP_OUT_KEY_SCHEMA) : getConf().get(
                Phase.MAP_OUT_VALUE_SCHEMA));
        
        boolean isProtobuf = !isKey && Message.class.isAssignableFrom(getMapOutClass(getConf()));
        
        DatumReader reader = null;
        if(isProtobuf)
        	reader = new ProtobufDatumReader<T>(schema);
        else if(isKey)
        	reader = new BinaryKeyDatumReader();
        else
        	reader = new ReflectDatumReader<T>(schema);
        
        return new AvroWrapperDeserializer(reader, isKey);
    }
    
    private static final Class<?> getMapOutClass(Configuration conf) {
        try {
            return conf.getClassByName(conf.get(Phase.MAP_OUT_CLASS));
        } catch(ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static final DecoderFactory FACTORY = new DecoderFactory();
//    static {
//        FACTORY.configureDirectDecoder(true);
//    }

    private class AvroWrapperDeserializer implements Deserializer<AvroWrapper<T>> {

        private DatumReader<T> reader;
        private InputStream in;
        private Decoder decoder;
        private boolean isKey;

        public AvroWrapperDeserializer(DatumReader<T> reader, boolean isKey) {
            this.reader = reader;
            this.isKey = isKey;
        }

        public void open(InputStream in) {
        	this.in = in;
        	this.decoder = FACTORY.directBinaryDecoder(in, null);
        }

        public AvroWrapper<T> deserialize(AvroWrapper<T> wrapper) throws IOException {
            T datum = reader.read(wrapper == null ? null : wrapper.datum(), decoder);
            if (wrapper == null) {
                wrapper = isKey ? new AvroKey<T>(datum) : new AvroValue<T>(datum);
            }
            else {
                wrapper.datum(datum);
            }
            return wrapper;
        }

        public void close() throws IOException {
        	if(decoder instanceof BinaryDecoder)
        		((BinaryDecoder) decoder).inputStream().close();
        	else
        		in.close();
        }
    }

    /** Returns the specified output serializer. */
    public Serializer<AvroWrapper<T>> getSerializer(Class<AvroWrapper<T>> c) {
        if(AvroKey.class.isAssignableFrom(c))
        	return new BinaryKeySerializer();
    	
        Configuration conf = getConf();
        // Here we must rely on mapred.task.is.map to tell whether the map output
        // or final output is needed.
        boolean isMap = conf.getBoolean("mapred.task.is.map", false);
       
        Schema schema = null;
        if(!isMap)
            schema = AvroJob.getOutputSchema(conf);
        else
            schema = Schema.parse(conf.get(Phase.MAP_OUT_VALUE_SCHEMA));
        
        boolean isProtobuf = Message.class.isAssignableFrom(getMapOutClass(getConf()));
        
        DatumWriter<T> writer;
        if(isProtobuf)
        	writer = new ProtobufDatumWriter<T>(schema);
        else
        	writer = new ReflectDatumWriter<T>(schema);
        
        return new AvroValueSerializer(writer);
    }
    
    private class BinaryKeySerializer implements Serializer<AvroWrapper<T>> {

    	OutputStream out;
    	
		@Override
		public void open(OutputStream out) throws IOException {
			this.out = out;
		}

		@Override
		public void serialize(AvroWrapper<T> t) throws IOException {
			BinaryKey key = (BinaryKey) t.datum();
			out.write(key.getBuffer(), 0, key.getLength());
		}

		@Override
		public void close() throws IOException {
			out.close();
		}
    }

    private class AvroValueSerializer implements Serializer<AvroWrapper<T>> {

        private DatumWriter<T> writer;
        private OutputStream out;
        private Encoder encoder;

        public AvroValueSerializer(DatumWriter<T> writer) {
            this.writer = writer;
        }

        public void open(OutputStream out) {
            this.out = out;
            this.encoder = new EncoderFactory().directBinaryEncoder(out, null);
        }

        public void serialize(AvroWrapper<T> wrapper) throws IOException {
            writer.write(wrapper.datum(), encoder);
        }

        public void close() throws IOException {
            out.close();
        }

    }
}
