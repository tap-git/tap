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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.mapred.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import tap.formats.avro.AvroGroupPartitioner;
import tap.formats.avro.TapAvroSerialization;
import tap.util.ReflectUtils;

import org.apache.avro.protobuf.ProtobufData;

import com.google.protobuf.Message;

import tap.util.ObjectFactory;

@SuppressWarnings("deprecation")
public class Phase {

    private static final int MAPPER2_IN_PARAMETER_POSITION = 0;
    private static final int MAPPER2_OUT_PARAMETER_POSITION = 1;
    private static final int REDUCER2_IN_PARAMETER_POSITION = 0;
    private static final int REDUCER2_OUT_PARAMETER_POSITION = 1;
    private static final String SETTINGS = "tap.phase.settings";
    public static final String MAPPER = "tap.phase.mapper";
    public static final String MAP_IN_CLASS = "tap.phase.map.input.class";
    public static final String MAP_OUT_KEY_SCHEMA = "tap.phase.map.out.key.schema";
    public static final String MAP_OUT_VALUE_SCHEMA = "tap.phase.map.out.value.schema";
    public static final String MAP_OUT_PIPE_CLASS = "tap.phase.map.output.pipe.class"; // The
                                                                                       // OUT
                                                                                       // in
                                                                                       // Pipe<OUT>
    public static final String MAP_OUT_CLASS = "tap.phase.map.output.class";
    public static final String REDUCER_OUT_PIPE_CLASS = "tap.phase.reduce.output.pipe.class";
    public static final String REDUCER = "tap.phase.reducer";
    public static final String REDUCE_OUT_CLASS = "tap.phase.reduce.output.class";
    public static final String GROUP_BY = "tap.phase.groupby";
    public static final String SORT_BY = "tap.phase.sortby";
    public static final String COMBINER = "tap.phase.combiner";

    /*
     * we *allow* for multiple reads, writes, maps, combines, and reduces this
     * would support *manual* optimization of merging we hope to never use them
     * - instead we'll have simple mappings and rely on an optimizer that will
     * let us do multi input-output
     */
    /** files read by main map/reduce pipeline */
    private List<Pipe> mainReads;
    /** any files read, including side writes */
    private List<Pipe> reads;
    /** files written by main map/reduce pipeline */
    private List<Pipe> mainWrites;
    /** any files written, including side writes */
    private List<Pipe> writes;
    private Class<? extends Mapper>[] mappers;
    private Class<? extends ColReducer>[] combiners;
    private Class<? extends ColReducer>[] reducers;
    private String groupBy;
    private String sortBy;
    private Map<String, String> props = new LinkedHashMap<String, String>();
    private String name;
    private JobConf conf;
    private Integer deflateLevel;
    private Map<String, String> textMeta = new TreeMap<String, String>();
    private Object reduceOutProto;
    private Class<?> reduceOutClass;
    private Schema reduceout;
    private Class<? extends Mapper> mapperClass;
    private Method mapperMethod;
    private Class<?> mapOutClass;
    private Class<?> mapInClass;
    private Schema mapin;
    private Class<? extends ColReducer> reducerClass;
    private Object inputPipeProto;
    private Class<?> mapOutPipeType;
    private Class<Pipe> reduceOutPipeType;
    private Class<?> reduceInClass;

    public Phase() {
    }

    public Phase(String name) {
        this.name = name;
    }

    public Pipe output() {
        return output(0);
    }

    public Pipe output(int n) {
        if (mainWrites == null) {
            // this *should* set up a promise to get the nth output ...
            throw new UnsupportedOperationException(
                    "please define outputs first, for now");
        }
        return mainWrites.get(n);
    }
    
    /**
     * The output of one phase is the input to another phase
     * @param phase The previous phase
     * @return This phase.
     */
    public Phase reads(Phase phase) {
        this.reads(phase.getOutputs().get(0));  //TODO: Only one input is supported
        return this;
    }

    public Phase reads(Pipe... inputs) {
        return reads(Arrays.asList(inputs));
    }

    public Phase reads(Collection<Pipe> inputs) {
        if (mainReads == null) {
            mainReads = new ArrayList<Pipe>(inputs);
        } else {
            mainReads.addAll(inputs);
        }
        return readsSide(inputs);
    }

    /**
     * side writes are files that are written but not as the output of a
     * map/reduce step, instead they are written by tasks or processes through
     * independent data path
     */
    public Phase readsSide(Pipe... sideFiles) {
        return readsSide(Arrays.asList(sideFiles));
    }

    /**
     * side writes are files that are written but not as the output of a
     * map/reduce step, instead they are written by tasks or processes through
     * independent data path
     */
    public Phase readsSide(Collection<Pipe> sideFiles) {
        if (this.reads == null) {
            this.reads = new ArrayList<Pipe>(sideFiles);
        } else {
            this.reads.addAll(sideFiles);
        }
        return this;
    }

    public Phase writes(Pipe... outputs) {
        return writes(Arrays.asList(outputs));
    }

    public Phase writes(Collection<Pipe> outputs) {
        writesSide(outputs);
        if (mainWrites == null) {
            mainWrites = new ArrayList<Pipe>(outputs);
        } else {
            mainWrites.addAll(outputs);
        }
        return this;
    }

    /**
     * side writes are files that are written but not as the output of a
     * map/reduce step, instead they are written by tasks or processes through
     * independent data path
     */
    public Phase writesSide(Pipe... sideFiles) {
        return writesSide(Arrays.asList(sideFiles));
    }

    /**
     * side writes are files that are written but not as the output of a
     * map/reduce step, instead they are written by tasks or processes through
     * independent data path
     */
    public Phase writesSide(Collection<Pipe> sideFiles) {
        for (Pipe file : sideFiles) {
            Phase p = file.getProducer();
            if (p != null && p != this) {
                throw new IllegalStateException("File " + file
                        + " has multiple producers " + this + ", " + p);
            }
            file.setProducer(this);
        }
        if (this.writes == null) {
            this.writes = new ArrayList<Pipe>(sideFiles);
        } else {
            this.writes.addAll(sideFiles);
        }
        return this;
    }

    /*
     * Specify the Mapper class
     */
    public Phase map(Class<? extends Mapper>... mappers) {
        this.mappers = mappers;
        return this;
    }

    public Phase combine(Class<? extends ColReducer>... combiners) {
        this.combiners = combiners;
        return this;
    }

    /*
     * Specify the Reducer class
     */
    public Phase reduce(Class<? extends ColReducer>... reducers) {
        this.reducers = reducers;
        return this;
    }

    public Phase groupBy(String groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    public Phase sortBy(String sortBy) {
        this.sortBy = sortBy;
        return this;
    }

    public Phase set(String key, String value) {
        props.put(key, value);
        return this;
    }

    public Phase setSettings(Object settings) {
        return setJson(SETTINGS, settings);
    }

    public Phase setJson(String key, Object value) {
        return set(key, toJson(value));
    }

    public Phase addMeta(String prefix, String value) {
        textMeta.put(prefix, value);
        return this;
    }

    private String toJson(Object value) {
        // TODO
        throw new UnsupportedOperationException("toJson not yet working");
    }

    public List<PhaseError> plan(Tap distPipeline) {
        List<PhaseError> errors = new ArrayList<PhaseError>();
        conf = new JobConf(distPipeline.getConf());
        for (Map.Entry<String, String> entry : props.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        try {
            mapperPlan(errors);
        } catch(Exception e) {
           errors.add(new PhaseError(e.toString()));
        }
        combinersPlan(errors);
        reducerPlan(errors);
        formatPlan(errors);

        Schema mapValueSchema = mapOutPlan(errors);

        configurationSetup(errors, mapValueSchema);
        return errors;
    }

    /**
     * @param errors
     * @return
     */
    private void formatPlan(List<PhaseError> errors) {
        Schema valueSchema = null;
        if (mainWrites.size() != 1) {
            errors.add(new PhaseError(
                    "Tap phase/avro currently only supports one output per process: "
                            + mainWrites));
        } else {
            Pipe output = mainWrites.get(0);
            AvroOutputFormat.setOutputPath(conf, new Path(output.getPath()));

            if (output.getPrototype() != null) {
                valueSchema = getSchema(output.getPrototype());
                if (reduceout != null) {
                    assert reduceout.equals(valueSchema); // should make an
                                                          // error not assert
                                                          // this!
                }
            } else {
                if (reduceout == null) {
                    errors.add(new PhaseError("No output format defined"));
                }
                valueSchema = reduceout;
            }
            output.setupOutput(conf);
            conf.set(AvroJob.OUTPUT_SCHEMA, valueSchema.toString());
        }

        if (deflateLevel != null)
            AvroOutputFormat.setDeflateLevel(conf, deflateLevel);

        inputPipeProto = null;
        if (mainReads != null && mainReads.size() > 0) {
            Path[] inPaths = new Path[mainReads.size()];
            int i = 0;
            for (Pipe pipe : mainReads) {
                inPaths[i++] = new Path(pipe.getPath());
                Object myProto = pipe.getPrototype();
                if (myProto == null) {
                    errors.add(new PhaseError(
                            "Pipes need non-null prototypes: " + pipe.getPath()));

                } else if (inputPipeProto != null) {
                    if (myProto.getClass() != inputPipeProto.getClass()) {
                        errors.add(new PhaseError(
                                "Inconsistent prototype classes for inputs: "
                                        + myProto.getClass() + " vs "
                                        + inputPipeProto.getClass() + " for "
                                        + pipe.getPath()));
                    }
                } else {
                    inputPipeProto = myProto;
                }
            }
            AvroInputFormat.setInputPaths(conf, inPaths);

            if (mapin == null) {
                if (inputPipeProto == null) {
                    errors.add(new PhaseError("Undefined input format"));
                } else {
                    mapin = getSchema(inputPipeProto);
                    mapInClass = inputPipeProto.getClass();
                }
            }
            mainReads.get(0).setupInput(conf);
            if (conf.get("mapred.input.format.class") == null)
                conf.setInputFormat(AvroInputFormat.class);
        }
    }

    /**
     * @param errors
     * @return
     */
    private void reducerPlan(List<PhaseError> errors) {
        reduceout = null;
        reduceOutClass = null;
        Class<? extends ColReducer> reducerClass = null;
        if (reducers != null && reducers.length > 0) {
            if (reducers.length != 1) {
                errors.add(new PhaseError(
                        "Tap Phase currently only supports one reducer per process: "
                                + name));
            } else {
                reducerClass = reducers[0];
                conf.set(REDUCER, reducers[0].getName());
                Class<?> foundIn = null;
                for (Method m : reducerClass.getMethods()) {
                    if ("reduce".equals(m.getName())
                            && !m.getDeclaringClass().equals(Object.class)
                            && !m.getDeclaringClass().equals(BaseReducer.class)) {
                        Class<?>[] paramTypes = m.getParameterTypes();
                        if (paramTypes.length == 2) {
                            if (paramTypes[1].equals(Pipe.class)) {
                                // found the correct map function
                                foundIn = m.getDeclaringClass();
                                this.reduceInClass = ReflectUtils
                                        .getParameterClass(foundIn,
                                                REDUCER2_IN_PARAMETER_POSITION);
                                this.reduceOutPipeType = Pipe.class;
                                this.reduceOutClass = ReflectUtils
                                        .getParameterClass(foundIn,
                                                REDUCER2_OUT_PARAMETER_POSITION);
                                break;
                            }
                        }
                        if (paramTypes.length >= 3) {
                            if (foundIn == null
                                    || foundIn.isAssignableFrom(m
                                            .getDeclaringClass())) {
                                if (foundIn == m.getDeclaringClass()
                                        && paramTypes[1] == Object.class) {
                                    // skip the generated "override" of the
                                    // generic method
                                    continue;
                                }
                                // prefer subclass methods to superclass methods
                                reduceOutClass = paramTypes[1];
                                foundIn = m.getDeclaringClass();
                            }
                        }
                    }
                }
            }
        }
        reduceOutProto = null;
        // TODO: handle cases beyond Object where output isn't defined
        if ((reduceOutClass == null || reduceOutClass == Object.class)
                && mainWrites != null && mainWrites.size() > 0) {
            reduceOutProto = mainWrites.get(0).getPrototype();
            reduceOutClass = reduceOutProto.getClass();
        } else {
            try {
                reduceOutProto = ObjectFactory.newInstance(reduceOutClass);
                Object fileProto = mainWrites.get(0).getPrototype();
                if (fileProto == null) {
                    mainWrites.get(0).setPrototype(reduceOutProto); // store
                                                                    // output
                                                                    // type
                                                                    // inferred
                                                                    // from
                                                                    // input
                } else if (fileProto.getClass() != reduceOutClass) {
                    errors.add(new PhaseError(
                            "Inconsistency in reducer output classes: "
                                    + fileProto.getClass() + " vs "
                                    + reduceOutClass));
                }
            } catch (Exception e) {
                errors.add(new PhaseError(e,
                        "Can't create reducer output class: " + reduceOutClass));
            }
        }
        if (reduceOutProto != null)
            reduceout = getSchema(reduceOutProto);
        if (reduceout == null && (reducers == null || reducers.length == 0)) {
            // default reducer - use mapper output
            reduceOutClass = mapOutClass;
            try {
                reduceOutProto = ObjectFactory.newInstance(reduceOutClass);
                reduceout = getSchema(reduceOutProto);
            } catch (Exception e) {
                errors.add(new PhaseError(e,
                        "Can't create reducer output class: " + reduceOutClass));
            }
        }
        conf.set(REDUCE_OUT_CLASS, reduceOutClass.getName());
    }
    
    
    /**
     * Find the correct Mapper class.
     * 
     * @param errors
     */
    private void mapperPlan(List<PhaseError> errors) throws Exception {
        mapin = null;
        mapOutClass = null;
        mapInClass = null;

        mapperClass = null;
        if (mappers != null && mappers.length > 0) {
            if (mappers.length > 1) {
                errors.add(new PhaseError(
                        "Tap Phase currently only supports one mapper per process: "
                                + name));
            } else {
                mapperClass = mappers[0];
                Class<?> foundIn = null;
                // search for suitable map(...) methods
                for (Method m : mapperClass.getMethods()) {
                    // ignore base classes Object and BaseMapper
                    if (!m.getDeclaringClass().equals(Object.class)
                            && !m.getDeclaringClass().equals(BaseMapper.class)
                            && "map".equals(m.getName())) {
                        Class<?>[] paramTypes = m.getParameterTypes();

                        /**
                         * map(IN,OutPipe<OUT>)
                         */
                        if (paramTypes.length == 2) {
                            if (paramTypes[1].equals(Pipe.class)) {
                                // found the correct map function
                                foundIn = m.getDeclaringClass();
                                this.mapInClass = ReflectUtils
                                        .getParameterClass(foundIn,
                                                MAPPER2_IN_PARAMETER_POSITION);
                                this.mapOutPipeType = Pipe.class;
                                this.mapOutClass = ReflectUtils
                                        .getParameterClass(foundIn,
                                                MAPPER2_OUT_PARAMETER_POSITION);

                                Object readProto = mainReads.get(0)
                                        .getPrototype();
                                if(readProto == null) {
                                    readProto = ObjectFactory.newInstance(mapInClass);
                                    mainReads.get(0).setPrototype(readProto);
                                }
                                
                                this.mapin = getSchema(readProto);

                                this.mapperMethod = m;
                                break;
                            }
                        }
                        /**
                         * Find legacy map(<IN> in, <OUT> out, TapContext<OUT>
                         * context)
                         */
                        if (paramTypes.length == 3
                                && paramTypes[2].equals(TapContext.class)) {

                            try {
                                // prefer subclass methods to superclass
                                // methods
                                if (foundIn == null
                                        || foundIn.isAssignableFrom(m
                                                .getDeclaringClass())) {
                                    if (paramTypes[0] == Object.class) {
                                        if (foundIn == m.getDeclaringClass()) {
                                            // skip the generated "override"
                                            // of the generic method
                                            continue;
                                        }
                                    } else {
                                        // TODO: handle cases beyond Object
                                        // where output isn't defined
                                        mapInClass = paramTypes[0];
                                        mapin = getSchema(ObjectFactory.newInstance(paramTypes[0]));
                                        Object readProto = mainReads.get(0).getPrototype();
                                        if (readProto == null) {
                                            readProto = ObjectFactory.newInstance(mapInClass);
                                            mainReads.get(0).setPrototype(readProto);
                                        }
                                        mapInClass = mapOutClass = readProto
                                                .getClass();
                                        mapin = getSchema(readProto);
                                    }
                                    mapOutClass = paramTypes[1];
                                    foundIn = m.getDeclaringClass();
                                }
                            } catch (Exception e) {
                                errors.add(new PhaseError(e,
                                        "Can't create mapper: " + mapperClass));
                            }
                        }

                    }
                }
            }
        }
    }

    /**
     * @param errors
     * @return
     */
    private Schema mapOutPlan(List<PhaseError> errors) {
        Schema mapValueSchema = null;
        try {
            // TODO: handle cases beyond Object where input isn't defined
            if (mapOutClass == null || mapOutClass == Object.class) {
                assert mapperClass == null;
                if (inputPipeProto != null) {
                    mapOutClass = inputPipeProto.getClass();
                    mapValueSchema = getSchema(inputPipeProto);
                } else {
                    // not available - try to get it from the reducer
                    if (reducerClass == null) {
                        mapOutClass = reduceOutClass;
                        mapValueSchema = getSchema(ObjectFactory.newInstance(reduceOutClass));
                    } else {
                        // can't get it from reducer input - that's just
                        // Iteratable
                        String fname = "no input file specified";
                        if (mainReads != null && mainReads.size() > 0)
                            fname = mainReads.get(0).getPath();
                        errors.add(new PhaseError(
                                "No input format specified for identity mapper - specify it on input file "
                                        + fname));
                    }
                }
            } else {
                mapValueSchema = getSchema(ObjectFactory.newInstance(mapOutClass));
            }
        } catch (Exception e) {
            errors.add(new PhaseError(e,
                    "Can't create instance of map output class: " + mapOutClass));
        }
        return mapValueSchema;
    }

    /**
     * Update Job Configuration with findings to be used by Mapper and Reducer
     * 
     * @param errors
     *            Configuration errors collection to be appended to
     * @param mapValueSchema
     */
    private void configurationSetup(List<PhaseError> errors,
            Schema mapValueSchema) {

        if (null == this.mapperClass) {
            errors.add(new PhaseError("Missing Mapper class"));
        } else {
            conf.set(MAPPER, this.mapperClass.getName());
        }

        if (null == this.mapInClass) {
            errors.add(new PhaseError("Missing map IN class"));
        } else {
            conf.set(MAP_IN_CLASS, mapInClass.getName());
        }

        if (null == this.mapOutClass) {
            errors.add(new PhaseError("Missing map OUT class"));
        } else {
            conf.set(MAP_OUT_CLASS, mapOutClass.getName());
        }

        // if we found piped map output type, set it here
        if (null != this.mapOutPipeType) {
            conf.set(MAP_OUT_PIPE_CLASS, this.mapOutPipeType.getName());
        }

        // if we found a piped reduce output type, set it here
        if (null != this.reduceOutPipeType) {
            conf.set(REDUCER_OUT_PIPE_CLASS, this.reduceOutPipeType.getName());
        }

        if (inputPipeProto != null) {
            conf.set(AvroJob.INPUT_SCHEMA, getSchema(inputPipeProto).toString());
        } else if (mapin != null) {
            conf.set(AvroJob.INPUT_SCHEMA, mapin.toString());
        } else {
            errors.add(new PhaseError("No map input defined"));
        }

        if (mapValueSchema != null) {
            conf.set(MAP_OUT_VALUE_SCHEMA, mapValueSchema.toString());
        }

        if (groupBy != null || sortBy != null) {
            conf.set(MAP_OUT_KEY_SCHEMA, group(mapValueSchema, groupBy, sortBy)
                    .toString());
        }
        if (groupBy != null) {
            conf.set(GROUP_BY, groupBy);
            AvroJob.setOutputMeta(conf, GROUP_BY, groupBy);
        }
        if (sortBy != null) {
            conf.setPartitionerClass(AvroGroupPartitioner.class);
            conf.set(SORT_BY, sortBy);
            AvroJob.setOutputMeta(conf, SORT_BY, sortBy);
        }

        conf.setMapOutputKeyClass(AvroKey.class);
        conf.setMapOutputValueClass(AvroValue.class);
        conf.setOutputKeyComparatorClass(KeyComparator.class);

        conf.setMapperClass(MapperBridge.class);
        conf.setReducerClass(ReducerBridge.class);

        for (Map.Entry<String, String> entry : textMeta.entrySet())
            AvroJob.setOutputMeta(conf, entry.getKey(), entry.getValue());

        // add TapAvroSerialization to io.serializations
        Collection<String> serializations = conf
                .getStringCollection("io.serializations");
        if (!serializations.contains(TapAvroSerialization.class.getName())) {
            serializations.add(TapAvroSerialization.class.getName());
            conf.setStrings("io.serializations",
                    serializations.toArray(new String[0]));
        }
    }

    /**
     * @param errors
     */
    private void combinersPlan(List<PhaseError> errors) {
        if (combiners != null && combiners.length > 0) {
            if (combiners.length > 1) {
                errors.add(new PhaseError(
                        "Tap Phase currently only supports one combiner per process: "
                                + name));
            } else {
                conf.set(COMBINER, combiners[0].getName());
                conf.setCombinerClass(CombinerBridge.class);
            }
        }
    }

    public static Schema getSchema(Object proto) {
        if(proto instanceof Message)
            return ProtobufData.get().getSchema(proto.getClass());
        try {
            Field schemaField = proto.getClass().getField("SCHEMA$");
            return (Schema) schemaField.get(null);
        } catch (NoSuchFieldException e) {
            // use reflection
            return ReflectData.get().getSchema(proto.getClass());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Provide accessor for testing
     * 
     * @return return this phase's configuration.
     */
    JobConf getConf() {
        return this.conf;
    }

    public PhaseError submit() {
        try {
            System.out.println("Submitting job:");
            System.out.println(getDetail());
            // probably should just make a conf right here?
            for (Pipe file : writes) {
                file.clearAndPrepareOutput(this.conf);
            }

            if (reads != null) {
                int i = 0;
                for (Pipe file : reads) {
                    // record inputs, to allow determination of obsolescence
                    // really we should be recording the transitive closure of
                    // dependencies here
                    // to allow determining files that are out of date with
                    // respect to their original source inputs
                    // although that could get costly for large numbers of log
                    // files...
                    AvroJob.setOutputMeta(conf, "input.file.name." + i,
                            file.getPath());
                    AvroJob.setOutputMeta(conf, "input.file.mtime." + i,
                            file.getTimestamp(conf));
                    i++;
                }
            }
            JobClient.runJob(conf);
            return null;
        } catch (Throwable t) {
            System.err.println("Job failure");
            t.printStackTrace();
            // clean up failed job output
            for (Pipe file : getOutputs()) {
                file.delete(conf);
            }
            return new PhaseError(t, name);
        }
    }

    public List<Pipe> getInputs() {
        return Collections.unmodifiableList(reads);
    }

    public List<Pipe> getOutputs() {
        return Collections.unmodifiableList(writes);
    }

    /**
     * create a schema containing just the listed comma-separated fields
     */
    public static Schema group(Schema schema, String... fields) {
        List<String> fieldList = new ArrayList<String>(fields.length);
        for (String list : fields) {
            if (list == null)
                continue;
            for (String field : list.split(",")) {
                field = field.trim();
                String[] parts = field.split("\\s");
                if (parts.length > 0) {
                    fieldList.add(parts[0]);
                }
            }
        }

        return group(schema, fieldList);
    }

    public static Schema group(Schema schema, List<String> fields) {
        ArrayList<Schema.Field> fieldList = new ArrayList<Schema.Field>(
                fields.size());
        StringBuilder builder = new StringBuilder();
        String missing = null;
        Set<String> held = new TreeSet<String>();
        for (String fieldname : fields) {
            if (held.contains(fieldname))
                continue;
            held.add(fieldname);
            Schema.Field field = schema.getField(fieldname.trim());
            if (field == null) {
                if (missing == null) {
                    missing = "Invalid group by/sort by - fields not in map output record are: ";
                } else {
                    missing += ", ";
                }
                missing += fieldname.trim();
                continue;
            }
            Schema.Field copy = new Schema.Field(fieldname, field.schema(),
                    field.doc(), field.defaultValue(), field.order());
            fieldList.add(copy);
            builder.append('_');
            builder.append(fieldname);
        }
        if (missing != null) {
            throw new IllegalArgumentException(missing);
        }
        schema = Schema.createRecord(
                schema.getName() + "_proj" + builder.toString(), "generated",
                schema.getNamespace(), false);
        schema.setFields(fieldList);
        return schema;
    }

    public String getSummary() {
        return "mapper " + getMapName() + " reading " + phaseReads()
                + " reducer " + getReduceName();
    }

    private String getReduceName() {
        return reducers == null || reducers[0] == null ? "identity"
                : reducers[0].getName();
    }

    private String getMapName() {
        return mappers == null || mappers[0] == null ? "identity" : mappers[0]
                .getName();
    }

    private String getDetail() {
        return String.format(
                "map: %s\nreduce: %s\nreading: %s\nwriting: %s\ngroup by:%s%s",
                getMapName(), getReduceName(), phaseReads(), mainWrites.get(0)
                        .getPath(), groupBy, (sortBy == null ? ""
                        : "\nsort by:" + sortBy));
    }

    private String phaseReads() {
        StringBuilder reading = new StringBuilder();
        for (Pipe read : mainReads) {
            if (reading.length() > 0)
                reading.append(',');
            reading.append(read.getPath());
        }
        return reading.toString();
    }

}
