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
package tap;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import tap.core.BinaryKeyComparator;
import tap.core.CombinerBridge;
import tap.core.MapperBridge;
import tap.core.ReducerBridge;
import tap.core.TapMapperInterface;
import tap.core.TapReducerInterface;
import tap.formats.avro.AvroGroupPartitioner;
import tap.formats.avro.BinaryKeyPartitioner;
import tap.formats.avro.TapAvroSerialization;
import tap.util.CacheUtils;
import tap.util.ReflectUtils;

import tap.util.ObjectFactory;

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
    
    public static final String MULTIPLE_OUTPUT_PREFIX = "tap.phase.multiple.output.prefix";

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
    private Class<? extends TapMapper>[] mappers;
    private Class<? extends TapReducer>[] combiners;
    private Class<? extends TapReducer>[] reducers;
    private String groupBy;
    private String sortBy;
    private Map<String, String> props = new LinkedHashMap<String, String>();
    private String name;
    private JobConf conf;
    private Integer deflateLevel;
    private Map<String, String> textMeta = new TreeMap<String, String>();

    private Class<? extends TapMapper> mapperClass;
    private Class<?> mapOutClass;
    private Class<?> mapInClass;
    private Schema mapinSchema; // Map IN parameter schema
    private Schema mapValueSchema;
    
    private Object reduceOutProto;
    private Class<?> reduceInClass;
    private Class<?> reduceOutClass;
    private Schema reduceout;
    private Class<? extends TapReducer> reducerClass;
	private Class<? extends TapReducer> combiner;

    private Object inputPipeProto;
    private Class<?> mapOutPipeType;
    private Class<Pipe> reduceOutPipeType;
    private HashMap<String, Serializable> mapperParameters = null;
    private int phaseID = -1;

	private Tap tap = null;

	/**
     * default constructor hidden to encourage use of the Tap.createPhase.
     */
    Phase() {
    }

    public Phase(String name) {
        this.name = name;
    }
    
    public Phase(Tap tap) {
    	this.tap = tap;
	}

	Pipe input() {
    	return input(0);
    }
    
    Pipe input(int n) {
	   if (mainReads == null) {
           // this *should* set up a promise to get the nth output ...
           throw new UnsupportedOperationException(
                   "please define input first, for now");
       }
       return mainReads.get(n);
    }

    /**
     * Returns the first and default output of this phase.
     * @return The output Pipe.
     */
    public Pipe<?> output() {
        return output(0);
    }

    /**
     * Return the Pipe at index n
     * @param n the index.
     * @return The Pipe.
     */
    Pipe<?> output(int n) {
        if (mainWrites == null) {
            // this *should* set up a promise to get the nth output ...
            throw new UnsupportedOperationException(
                    "please define outputs first, for now");
        }
        return mainWrites.get(n);
    }
    
	/**
	 * The output of one phase is the input to another phase. This is most often
	 * used when connecting the output of one phase to the next phase.
	 * Chaining
	 * @param phase
	 *            The previous phase
	 * @return This phase.
	 */
	public Phase reads(Phase phase) {
		if (null == phase.getOutputs()) {
			phase.writes(phase);
		}
		reads(phase.getOutputs().get(0)); // TODO: Only one input is supported
		return this;
	}
    
    /**
     * Generate a temporary filename
     * @return The filename
     */
    String getTmpOutputName() {
    	return "/tmp/tap/"
    			+ (this.getConf() == null ? System.getProperty("user.name") : getConf().getUser())
    			//+ "/" + (new Date().getTime()) 
    			+ "/tap-" + tap.getName()
    			+ "-phase-output-" 
    			+ getName();
    }
    
    /**
     * Accessor for Phase name
     * @return The name
     */
    public String getName() {
    	if (null == name) {
    		setName("phase-" + getID());
    	}
    	return name;
    }
    
    /**
     * Set the Phase name
     * @param name The name
     * @return This Phase
     */
    public Phase setName(String name) {
    	this.name = name;
    	return this;
    }
    
    private synchronized int getID() {
    	if (-1 == phaseID) {
    		if (null != tap) {
    			phaseID = ++tap.globalPhaseID;
    		} else {
    			phaseID++;
    		}
    	}
    	return phaseID;
    }

    /**
	 * Set reads for this phase.
	 * @param inputs
	 * @return
	 */
    public Phase reads(Pipe<?>... inputs) {
        return reads(Arrays.asList(inputs));
    }
    
    /**
     * Convert list of strings into pipes and then add them as reads.
     * @param inputs filepath to read (file or directory)
     * @return
     */
    public Phase reads(String input) {
    	return reads(new Pipe(input));
    }

    /**
     * 
     * @param inputs
     * @return
     */
    public Phase reads(Collection<Pipe<?>> inputs) {
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
    public Phase readsSide(Pipe<?>... sideFiles) {
        return readsSide(Arrays.asList(sideFiles));
    }

    /**
     * side writes are files that are written but not as the output of a
     * map/reduce step, instead they are written by tasks or processes through
     * independent data path
     */
    public Phase readsSide(Collection<Pipe<?>> sideFiles) {
        if (this.reads == null) {
            this.reads = new ArrayList<Pipe>(sideFiles);
        } else {
            this.reads.addAll(sideFiles);
        }
        return this;
    }

    /**
     * Sets the phase output.
     * @param outputs
     * @return
     */
    public Phase writes(Pipe... outputs) {
        return writes(Arrays.asList(outputs));
    }

    /**
     * Sets the phase output.
     * @param outputs
     * @return
     */
    public Phase writes(Collection<Pipe> outputs) {
        writesSide(outputs);
        for(Pipe p: outputs) {
        	setMainWrites(p);
        }
        bindPhaseToPipes();
        return this;
    }

	private void setMainWrites(Pipe pipe) {
		if (mainWrites == null) {
            mainWrites = new ArrayList<Pipe>();
        }
        mainWrites.add(pipe);
	}
	
	/**
	 * Used to connect the input of one phase to the output of another phase.
	 * This API generates a temporary file.
	 * 
	 * @param nextPhase
	 * @return
	 */
	Phase writes(Phase nextPhase) {
		Pipe pipe = new Pipe(true);
		setMainWrites(pipe);
		setWrites(pipe);
		bindPhaseToPipes();
		return this;
	}
	
	/**
	 * Create pipe from path and add pipe to mainWrites
	 * @param path Pipe file path
	 * @return This phase
	 */
    public Phase writes(String path) {
     	Pipe pipe = new Pipe(path);
     	
     	setMainWrites(pipe);

    	setWrites(pipe);
    	
    	bindPhaseToPipes();
 
    	return this;
    }

	private void setWrites(Pipe pipe) {
		if (null == writes) {
    		writes = new ArrayList<Pipe>();
    	}
    	writes.add(pipe);    //??
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

    /**
	 * Set the producer on all of the output pipes.
	 */
	private void bindPhaseToPipes() {
		// bind pipes to this phase
	    for (Pipe pipe: mainWrites) {
	    	pipe.setProducer(this);
	    }
	}

	/*
     * Specify the Mapper class
     */
    public Phase map(Class<? extends TapMapper>... mappers) {
        this.mappers = mappers;
        return this;
    }

    public Phase combine(Class<? extends TapReducer>... combiners) {
        this.combiners = combiners;
        return this;
    }

    /*
     * Specify the Reducer class
     */
    public Phase reduce(Class<? extends TapReducer>... reducers) {
        this.reducers = reducers;
        return this;
    }
    
    public Phase multipleOutput(String prefix) {
        set(MULTIPLE_OUTPUT_PREFIX, prefix);
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
    
    private HashMap getMapperParameters() {
    	if (null == mapperParameters) {
    		mapperParameters = new HashMap<String, Serializable>();
    	}
    	return mapperParameters;
    }
    
    /**
     * Accept arbitrary Serializable object to be passed to Mapper
     * @param key String key to Value passed to Mapper
     * @param value Serializable Value to pass to Mapper.
     * @return Phase
     */
    public Phase set(String key, Serializable value) {
    	getMapperParameters().put(key, value);
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
        throw new UnsupportedOperationException("toJson not yet working");
    }

    /**
     * Generate plan for each phase in the Tap
     * - Sniff Output Class type
     * - Compute Output Format
     * - Set Output Format
     * - File stat output (exists, time stamp)
     * - Dependency calculation (abort if up to date)
     * - Prepare output (remove target)
     * - Sniff Input Class type
     * - Sniff Input File
     * -- File stat (time, exists...)
     * -- File, Files, Directory, or Partion set
     * -- Format
     * - Compute Input Format
     * - Setup Input Format
     * Rules
     * -- Mapper OUT == Reducer IN
     * -- Combiner IN == Reducer IN
     * -- Combiner OUT == Reducer OUT
     * @param tap
     * @return List of Phase errors
     */
    public List<PhaseError> plan(Tap tap) {
        List<PhaseError> errors = new ArrayList<PhaseError>();
        conf = new JobConf(tap.getConf());
 
        addParameters(errors);
        inputPlan(errors);
        outputPlan(errors);
        
        mapperPlan(errors);
        combinersPlan(errors);
        reducerPlan(errors);
        formatPlan(errors);
        mapOutPlan(errors);
		syntaxCheck(errors);
		if (0 == errors.size()) {
			configurationSetup(errors);
		}
        return errors;
    }

	private void addParameters(List<PhaseError> errors) {
		for (Map.Entry<String, String> entry : props.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        
        // add all mapper parameters to distributed cache
		if (null != mapperParameters) {
			for (Entry<String, Serializable> entry : mapperParameters
					.entrySet()) {
				try {
					CacheUtils.addMapToCache(entry.getKey(), entry.getValue(), conf);
				} catch (IOException e) {
					errors.add(new PhaseError(e.toString()));
				}
			}
		}
	}

    private void inputPlan(List<PhaseError> errors) {
		for (Pipe<?> file : reads) {
	     	file.setConf(conf);
	     	if (!file.isValidInput()) {
	     		errors.add(new PhaseError(file.getPath() + " is invalid"));
	    	}
	     }
	}

	private void outputPlan(List<PhaseError> errors) {
		 for (Pipe<?> file : writes) {
	     	file.setConf(conf);
	     	if (file.isTempfile && file.getPath() == null) {
	     		file.setPath(this.getTmpOutputName());
	     		break;
	     	}
	     	if (file.exists() && file.stat().isFile) {
	     		errors.add(new PhaseError(file.getPath() 
	     				+ " output is invalid, should be a directory and not a file"));
	     	}
	     }
	}

	/**
	 * Find the correct Mapper class.
	 * 
	 * @param errors
	 */
	private void mapperPlan(List<PhaseError> errors) {
	    mapperClass = null;
	
	    if (mappers == null || mappers.length != 1) {
	        errors.add(new PhaseError(
	                "Tap Phase currently requires exactly one mapper per process: "
	                        + name));
	        return;
	    }
	    mapperClass = mappers[0];
	    findMapperMethod(errors, mapperClass, "map");
	
	}

	/**
	 * @param errors
	 * @param mapperClass
	 * @param methodName
	 * @return
	 */
	private void findMapperMethod(List<PhaseError> errors,
			Class<? extends TapMapperInterface> mapperClass, String methodName) {
		try {
			mapinSchema = null;
			mapOutClass = null;
			mapInClass = null;
			Class<?> foundIn = null;
			for (Method m : mapperClass.getMethods()) {
				foundIn = m.getDeclaringClass();
	
				// ignore base classes Object and BaseMapper
				if (!foundIn.equals(Object.class)
						&& !foundIn.equals(TapMapper.class)
						&& methodName.equals(m.getName())) {
					Class<?>[] paramTypes = m.getParameterTypes();
	
					/**
					 * map(IN, Pipe<OUT>)
					 */
					if (paramTypes.length == 2
							&& paramTypes[1].equals(Pipe.class)) {
	
						// found the correct map function
						this.mapInClass = ReflectUtils.getParameterClass(
								foundIn, MAPPER2_IN_PARAMETER_POSITION);
						this.mapOutPipeType = Pipe.class;
						this.mapOutClass = ReflectUtils.getParameterClass(
								foundIn, MAPPER2_OUT_PARAMETER_POSITION);
	
						Pipe inpipe = mainReads.get(0);
						Object readProto = inpipe.getPrototype();
						if (readProto == null || readProto == "") {
							readProto = ObjectFactory.newInstance(mapInClass);
							inpipe.setPrototype(readProto);
						}
						
						this.mapinSchema = ReflectUtils.getSchema(readProto);
						break;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			errors.add(new PhaseError(e.getMessage()));
		}
	}

	/**
	 * @param errors
	 * @return
	 */
	private Schema mapOutPlan(List<PhaseError> errors) {
		try {
			// TODO: handle cases beyond Object where input isn't defined
			if (!(mapOutClass == null || mapOutClass == Object.class)) {
				mapValueSchema = ReflectUtils.getSchema(ObjectFactory
						.newInstance(mapOutClass));
			} else {
				if (inputPipeProto != null) {
					mapOutClass = inputPipeProto.getClass();
					mapValueSchema = ReflectUtils.getSchema(inputPipeProto);
				} else {
					// not available - try to get it from the reducer
					if (reducerClass != null) {
						mapOutClass = reduceOutClass;
						mapValueSchema = ReflectUtils.getSchema(ObjectFactory
								.newInstance(reduceOutClass));
					} else {
						// can't get it from reducer input - that's just
						// an Iterator
						String fname = "no input file specified";
						if (mainReads != null && mainReads.size() > 0) {
							fname = mainReads.get(0).getPath();
							// TODO: need to find mapValueSchema from file as last resort.
						} else {
							errors.add(new PhaseError(
									"No input format specified for identity mapper - specify it on input file "
											+ fname));
						}
					}
				}
			}
		} catch (Exception e) {
			errors.add(new PhaseError(
					"Can't create instance of map output class: " + mapOutClass));
		}
		return mapValueSchema;
	}

	/**
     * @param errors
     * @return
     */
    private void formatPlan(List<PhaseError> errors) {
        Schema valueSchema = null;
        if (null == mainWrites || mainWrites.size() != 1) {
            errors.add(new PhaseError(
                    "Tap phase/avro currently only supports one output per process: "
                            + mainWrites));
        } else {
            Pipe output = mainWrites.get(0);
            AvroOutputFormat.setOutputPath(conf, new Path(output.getPath()));

            if (output.getPrototype() != null) {
                valueSchema = ReflectUtils.getSchema(output.getPrototype());
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
            if (null != valueSchema) {
            	conf.set(AvroJob.OUTPUT_SCHEMA, valueSchema.toString());
            }
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

            if (mapinSchema == null) {
                if (inputPipeProto == null) {
                    errors.add(new PhaseError("Undefined input format"));
                } else {
                    mapinSchema = ReflectUtils.getSchema(inputPipeProto);
                    mapInClass = inputPipeProto.getClass();
                }
            }
            mainReads.get(0).setupInput(conf);
            if (conf.get("mapred.input.format.class") == null) {
                conf.setInputFormat(AvroInputFormat.class);
            }
        }
    }

    /**
     * @param errors
     * @return
     */
	private void reducerPlan(List<PhaseError> errors) {
		reduceout = null;
		reduceOutClass = null;
		
		if (reducers == null || reducers.length != 1) {
			errors.add(new PhaseError(
					"Tap Phase currently requires exactly one reducer per process: "
							+ name));
			return;
		} 
		reducerClass = reducers[0];
		
		String methodName = "reduce";
		findReducerMethod(reducerClass, methodName);

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
			reduceout = ReflectUtils.getSchema(reduceOutProto);
		if (reduceout == null && (reducers == null || reducers.length == 0)) {
			// default reducer - use mapper output
			reduceOutClass = mapOutClass;
			try {
				reduceOutProto = ObjectFactory.newInstance(reduceOutClass);
				reduceout = ReflectUtils.getSchema(reduceOutProto);
			} catch (Exception e) {
				errors.add(new PhaseError(e,
						"Can't create reducer output class: " + reduceOutClass));
			}
		}
	}

	private Class<?> findReducerMethod(
			Class<? extends TapReducerInterface> reducerClass,
			String methodName) {
		Class<?> foundIn = null;
		for (Method m : reducerClass.getMethods()) {
			foundIn = m.getDeclaringClass();
			if (methodName.equals(m.getName())
					&& !foundIn.equals(Object.class)
					&& !foundIn.equals(TapReducer.class)) {
				Class<?>[] paramTypes = m.getParameterTypes();
				if (paramTypes.length == 2) {
					if (paramTypes[1].equals(Pipe.class)) {
						// found the correct map function
						
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
			}
		}
		return foundIn;
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
	            return;
	        }
		    this.combiner = combiners[0];
	    }
	}
	
	private int syntaxCheck(List<PhaseError> errors) {
		
		if (null == this.writes || writes.isEmpty()) {
			errors.add(new PhaseError("Missing a writable output"));
		}
		if (null == this.reads || reads.isEmpty()) {
			errors.add(new PhaseError("Missing a readable input"));
		}
		if (null == this.mapperClass) {
			errors.add(new PhaseError("Missing Mapper class"));
		}
		if (null == this.mapInClass) {
			errors.add(new PhaseError("Missing map IN class"));
		}
	
		if (null == this.mapOutClass) {
			errors.add(new PhaseError("Missing map OUT class"));
		}
	
		if (inputPipeProto == null || mapinSchema == null) {
			errors.add(new PhaseError("No map input defined"));
		}
		if (mapValueSchema == null) {
			errors.add(new PhaseError("No map input defined"));
		}
		
		if (null == this.reducerClass) {
			errors.add(new PhaseError("Missing Reducer class"));
		}
		
		if (null == this.reduceInClass) {
			errors.add(new PhaseError("Missing Reducer IN class"));
		}
	
		if (null == this.reduceOutClass) {
			errors.add(new PhaseError("Missing Reducer OUT class"));
		}
	
		if (null == this.reduceOutPipeType) {
			errors.add(new PhaseError("No reduce output Pipe type defined"));
		}
		
		if (mapOutClass != reduceInClass) {
			errors.add(new PhaseError("Mis-match in Mapper OUT class and Reducer IN Class"));
		}
	
		if (groupBy == null && sortBy == null) {
			errors.add(new PhaseError("No groupBy or sortBy defined"));
		}
		return errors.size();
	}

	/**
     * Update Job Configuration with findings to be used by Mapper and Reducer
     * 
     * @param errors
     *            Configuration errors collection to be appended to
     * @param mapValueSchema
     */
    private void configurationSetup(List<PhaseError> errors) {

    	conf.setJobName(getName());
        conf.set(MAPPER, mapperClass.getName());
        conf.set(MAP_IN_CLASS, mapInClass.getName());
        conf.set(MAP_OUT_CLASS, mapOutClass.getName());
        
        conf.set(REDUCER_OUT_PIPE_CLASS, this.reduceOutPipeType.getName());
        conf.set(REDUCER, reducerClass.getName());
		conf.set(REDUCE_OUT_CLASS, reduceOutClass.getName());
		
		//Combiner is Optional
		if (null != combiner) {
	        conf.set(COMBINER, combiner.getName());
	        conf.setCombinerClass(CombinerBridge.class);
		}
		
        conf.set(AvroJob.INPUT_SCHEMA, ReflectUtils.getSchema(inputPipeProto).toString());
        conf.set(AvroJob.INPUT_SCHEMA, mapinSchema.toString());
        conf.set(MAP_OUT_VALUE_SCHEMA, mapValueSchema.toString());
        conf.set(MAP_OUT_KEY_SCHEMA, groupAndSort(mapValueSchema, groupBy, sortBy)
                .toString());
        
		
        // if we found piped map output type, set it here
        if (null != this.mapOutPipeType) {
            conf.set(MAP_OUT_PIPE_CLASS, this.mapOutPipeType.getName());
        }

        if (groupBy != null) {
            conf.set(GROUP_BY, groupBy);
            AvroJob.setOutputMeta(conf, GROUP_BY, groupBy);
        }
        if (sortBy != null) {
            conf.setPartitionerClass(BinaryKeyPartitioner.class);
            conf.set(SORT_BY, sortBy);
            AvroJob.setOutputMeta(conf, SORT_BY, sortBy);
        }

        conf.setMapOutputKeyClass(AvroKey.class);
        conf.setMapOutputValueClass(AvroValue.class);
        conf.setOutputKeyComparatorClass(BinaryKeyComparator.class);

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
     * Provide accessor for testing
     * 
     * @return return this phase's configuration.
     */
    JobConf getConf() {
        return this.conf;
    }

    PhaseError submit() {
        try {
            System.out.println("Submitting job:");
            System.out.println(getDetail());
            // probably should just make a conf right here?
            for (Pipe file : writes) {
                file.clearAndPrepareOutput();
            }

            if (reads != null) {
                int i = 0;
                for (Pipe file : reads) {
                	if (!file.isValidInput()) {
                		tap.getAlerter().alert("Phase.submit: " 
                				+ file.getPath() + " is invalid.");
                		//return null;
                		//throw new IllegalArgumentException(file.getPath() + " is invalid");
                	}
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
                            file.getTimestamp());
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
                file.delete();
            }
            return new PhaseError(t, name);
        }
    }

    /**
     * Return list of input Pipes
     * @return The unmodifiable List of Pipes, or null if no outputs have been specified.
     */
    public List<Pipe> getInputs() {
    	if (null == reads) {
    		return null;
    	}
        return Collections.unmodifiableList(reads);
    }

    /**
     * Return a list of input Pipes
     * @return The unmodifiable List of Pipes, or null if no outputs have been specified.
     */
    public List<Pipe> getOutputs() {
    	if (null == writes) {
    		return null;
    	}
        return Collections.unmodifiableList(writes);
    }
    
    public static Schema group(Schema schema, String... fields) {
    	StringBuilder sb = new StringBuilder();
    	for(String f : fields) {
    		if(f != null) {
    			sb.append(",");
    			sb.append(f);
    		}
    	}
    	return createSchema(schema, sb.toString(), "");
    }
    
    public static Schema groupAndSort(Schema schema, String groupFields, String sortFields) {
    	return createSchema(schema, groupFields, sortFields);
    }
    
    private static class FieldDesc {
    	final String name;
    	final boolean isSort;
    	Schema.Field.Order order;
    	
    	FieldDesc(String name, boolean isSort) {
    		this.name = name;
    		this.isSort = isSort;
    	}
    }
    
    private static List<FieldDesc> parseFields(String groupFields, String sortFields) {
    	groupFields = groupFields == null ? "" : groupFields.trim();
    	sortFields = sortFields == null ? "" : sortFields.trim();
    	
    	Map<String, FieldDesc> lookup = new HashMap<String, FieldDesc>();
    	List<FieldDesc> list = new ArrayList<FieldDesc>();
    	
    	boolean isSort = false;
    	for(String fields : new String[] { groupFields, sortFields }) {
    		for(String s : fields.split(",")) {
    			s = s.trim();
    			if(s.length() == 0)
    				continue;
    			
                String[] parts = s.split("\\s");
                String name = parts[0];
                
                FieldDesc field = lookup.get(name.toLowerCase());
                if(field == null) {
                	field = new FieldDesc(name, isSort);
                	list.add(field);
                	lookup.put(name.toLowerCase(), field); 
                }
                
                // update order with latest for cases like groupBy("word") sortBy("word desc")
                if(parts.length > 1 && parts[1].trim().toLowerCase().equals("desc")) {
                	field.order = Field.Order.DESCENDING;
                } else {
                	field.order = Field.Order.ASCENDING;
                }
    		}
    		isSort = true;
    	}
    	return list;
    }
    
    private static Schema createSchema(Schema schema, String groupFields, String sortFields) { 
        List<FieldDesc> parsed = parseFields(groupFields, sortFields);
        
        ArrayList<Schema.Field> fieldList = new ArrayList<Schema.Field>();
        String missing = null;
        StringBuilder builder = new StringBuilder();
        for(FieldDesc fd : parsed) {
            Schema.Field field = schema.getField(fd.name);
            if (field == null) {
                if (missing == null) {
                    missing = "Invalid group by/sort by - fields not in map output record are: ";
                } else {
                    missing += ", ";
                }
                missing += fd.name;
                continue;
            }
            Schema.Field copy = new Schema.Field(field.name(), field.schema(),
                    field.doc(), field.defaultValue(), fd.order);
            if(fd.isSort) {
            	copy.addProp("x-sort", "true");
            }
            fieldList.add(copy);
            builder.append('_');
            builder.append(fd.name);
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
