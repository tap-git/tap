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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.mapred.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.JsonParseException;

import tap.formats.FileFormat;
import tap.formats.Formats;
import tap.formats.avro.JsonToGenericRecord;
import tap.formats.text.TextFormat;

import com.twitter.elephantbird.mapreduce.input.LzoInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

@SuppressWarnings("deprecation")
public class MapperBridge<KEY, VALUE, IN, OUT, KO, VO> extends MapReduceBase
        implements org.apache.hadoop.mapred.Mapper<KEY, VALUE, KO, VO> {

    private static final int SNIFF_HEADER_SIZE = 1000;
    private Mapper<IN, OUT> mapper;
    private boolean isMapOnly;
    private OUT out;
    private TapContext<OUT> context;
    private Schema schema;
    private String groupBy;
    private String sortBy;
    private boolean isTextInput = false;
    private boolean isStringInput = false;
    private boolean isJsonInput = false;
    private boolean isProtoInput = false;
    private Schema inSchema;
    private int parseErrors = 0;
    private BinaryEncoder encoder = null;
    private EncoderFactory factory = new EncoderFactory();
    // TODO: make this configurable
    private int maxAllowedErrors = 1000;
    private OutPipe<OUT> outPipe = null;
    private boolean isPipeOutput = false;

    @SuppressWarnings("unchecked")
    public void configure(JobConf conf) {
        this.mapper = ReflectionUtils.newInstance(
                conf.getClass(Phase.MAPPER, BaseMapper.class, Mapper.class),
                conf);
        this.isMapOnly = conf.getNumReduceTasks() == 0;
        try {
            determineInputFormat(conf);
            determineOutputFormat(conf);
            this.groupBy = conf.get(Phase.GROUP_BY);
            this.sortBy = conf.get(Phase.SORT_BY);
        } catch (Exception e) {
            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            throw new RuntimeException(e);
        }

        mapper.setConf(conf);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void map(KEY wrapper, VALUE value,
            OutputCollector<KO, VO> collector, Reporter reporter)
            throws IOException {
        if (this.context == null) {
            KeyExtractor<GenericData.Record, OUT> extractor = new ReflectionKeyExtractor<OUT>(
                    schema, groupBy, sortBy);
            this.context = new TapContext<OUT>(new Collector(collector,
                    extractor), reporter);
        }

        bindContextToPipe(collector, reporter);

        invokeMapper(wrapper, value, reporter);
    }

    /**
     * @param conf
     * @throws IOException
     * @throws FileNotFoundException
     */
    private void determineInputFormat(JobConf conf)
            throws FileNotFoundException, IOException {
        
        /**
         * Compare mapper input file signature with Hadoop configured class
         */
        FileFormat ff = sniffMapInFormat(conf);
        if (!ff.isCompatible(conf.getInputFormat())) {
            throw new IllegalArgumentException("Map input format not compatible with file format.");
        }

        
        if (conf.getInputFormat() instanceof TextInputFormat) {
            Class<?> inClass = conf.getClass(Phase.MAP_IN_CLASS, Object.class,
                    Object.class);
            if (inClass == String.class) {
                isStringInput = true;
            } else if (inClass == Text.class) {
                isTextInput = true;
            } else {
                isJsonInput = true;
                inSchema = Phase.getSchema((IN) ReflectionUtils.newInstance(
                        inClass, conf));
            }
        }
        isProtoInput = conf.getInputFormat() instanceof LzoInputFormat;
    }

    /**
     * @param conf
     */
    private void determineOutputFormat(JobConf conf) {
        this.out = (OUT) ReflectionUtils.newInstance(
                conf.getClass(Phase.MAP_OUT_CLASS, Object.class, Object.class),
                conf);
        if (null != conf.get(Phase.MAP_OUT_PIPE_CLASS)) {
            this.outPipe = new OutPipe<OUT>(this.out);
            this.isPipeOutput = true;
        }
        this.schema = Phase.getSchema(this.out);
    }

    @SuppressWarnings("unchecked")
    private class Collector<K> extends AvroCollector<OUT> {
        private final AvroWrapper<OUT> wrapper = new AvroWrapper<OUT>(null);
        private final AvroKey<K> keyWrapper = new AvroKey<K>(null);
        private final AvroValue<OUT> valueWrapper = new AvroValue<OUT>(null);
        private final KeyExtractor<K, OUT> extractor;
        private final K key;
        private OutputCollector<KO, VO> collector;

        public Collector(OutputCollector<KO, VO> collector,
                KeyExtractor<K, OUT> extractor) {
            this.collector = collector;
            this.extractor = extractor;
            key = extractor.getProtypeKey();
            keyWrapper.datum(key);
        }

        public void collect(OUT datum) throws IOException {
            if (isMapOnly) {
                wrapper.datum(datum);
                collector.collect((KO) wrapper, (VO) NullWritable.get());
            } else {
                extractor.setKey(datum, key);
                valueWrapper.datum(datum);
                collector.collect((KO) keyWrapper, (VO) valueWrapper);
            }
        }
    }

    /**
     * @param wrapper
     * @param value
     * @param reporter
     * @throws IOException
     */
    private void invokeMapper(KEY wrapper, VALUE value, Reporter reporter)
            throws IOException {
        if (isTextInput) {
            mapper.map((IN) value, out, context);
        } else if (isStringInput) {
            mapper.map((IN) ((Text) value).toString(), out, context);
        } else if (isJsonInput) {
            String json = ((Text) value).toString();
            if (shouldSkip(json))
                return;
            // inefficient implementation of json to avro...
            // more efficient would be JsonToClass.jsonToRecord:
            // mapper.map((IN) JsonToClass.jsonToRecord(json, inSchema), out,
            // context);

            // silly conversion approach - serialize then deserialize
            try {
                GenericContainer c = JsonToGenericRecord.jsonToRecord(json,
                        inSchema);
                GenericDatumWriter<GenericContainer> writer = new GenericDatumWriter<GenericContainer>(
                        inSchema);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                writer.setSchema(inSchema);
                encoder = factory.binaryEncoder(bos, encoder);
                writer.write(c, encoder);
                byte[] data = bos.toByteArray();

                GenericDatumReader<IN> reader = new SpecificDatumReader<IN>(
                        inSchema);
                reader.setSchema(inSchema);

                IN converted = reader.read(null, DecoderFactory
                        .defaultFactory().createBinaryDecoder(data, null));

                mapper.map(converted, out, context);
            } catch (JsonParseException jpe) {
                System.err.println("Failed to parse " + json + ": "
                        + jpe.getMessage());
                reporter.incrCounter("ColHadoopMapper", "json-parse-error", 1L);
                if (++parseErrors > maxAllowedErrors) {
                    throw new RuntimeException(jpe);
                }
            }
        } else {
            if (this.isPipeOutput) {
                mapper.map(((AvroWrapper<IN>) wrapper).datum(), this.outPipe);
            } else {
                mapper.map(((AvroWrapper<IN>) wrapper).datum(), out, context);
            }
        }
    }

    /**
     * Bind the output to the Tap Context
     * 
     * @param collector
     * @param reporter
     */
    private void bindContextToPipe(OutputCollector<KO, VO> collector,
            Reporter reporter) {
        if (this.outPipe != null && this.outPipe.getContext() == null) {
            if (this.context == null) {
                KeyExtractor<GenericData.Record, OUT> extractor = new ReflectionKeyExtractor<OUT>(
                        schema, groupBy, sortBy);
                this.outPipe.setContext(new TapContext<OUT>(new Collector(
                        collector, extractor), reporter));
            } else {
                this.outPipe.setContext(this.context);
            }
        }
    }

    private boolean shouldSkip(String json) {
        int i;
        int len = json.length();
        for (i = 0; i < len; i++)
            if (!Character.isWhitespace(json.charAt(i)))
                break;
        if (i == len)
            return true; // blank line
        return (json.charAt(i) == '#' || json.charAt(i) == '/' && len > (i + 1)
                && json.charAt(i + 1) == '/'); // skip comments
    }

    /**
     * Open file and read header to determine file format
     * 
     * @param conf
     * @throws IOException
     * @throws FileNotFoundException
     */
    private FileFormat sniffMapInFormat(JobConf conf) throws IOException,
            FileNotFoundException {
        {
            FileFormat returnFormat = Formats.UNKNOWN_FORMAT.getFileFormat();
            Path path = new Path(conf.get("map.input.file"));
            File file = FileSystem.getLocal(conf).pathToFile(path);
            byte[] header = readHeader(file);
            returnFormat = determineFileFormat(header);
            
            InputFormat inputFormat = conf.getInputFormat();
            
            /*
            System.out
                    .println("tap.core.MapperBridge: local file path " + path);
            System.out.println("tap.core.MapperBridge: File format "
                    + returnFormat.toString());
            System.out.println("tap.core.MapperBridge: format extension "
                    + returnFormat.fileExtension());
            */
            return returnFormat;
        }
    }

    /**
     * Based on file header values return File format.
     * 
     * @param returnFormat
     * @param header
     * @return
     */
    private FileFormat determineFileFormat(byte[] header) {
        for (Formats format : Formats.values()) {
            if (format.getFileFormat().signature(header)) {
                return format.getFileFormat();

            }
        }
        return Formats.UNKNOWN_FORMAT.getFileFormat();
    }

    /**
     * Read first N bytes from normal file system file.
     * @param file
     * @return byte buffer containing first SNIFF_HEADER_SIZE bytes.
     * @throws FileNotFoundException
     * @throws IOException
     */
    private byte[] readHeader(File file) throws FileNotFoundException,
            IOException {
        InputStream inputStream = new FileInputStream(file);
        byte[] header = new byte[SNIFF_HEADER_SIZE];
        inputStream.read(header);
        inputStream.close();
        return header;
    }

    @Override
    public void close() throws IOException {
        mapper.close(out, context);
    }
}
