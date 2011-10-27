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
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufB64LineInputFormat;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufB64LineOutputFormat;

@SuppressWarnings("deprecation")
public class Pipe<T> {

    private Phase producer;
    private String path;
    private T prototype;
    private Formats format = Formats.AVRO_FORMAT;
    private Class<?> protoClass;

    public static enum Formats {
        STRING_FORMAT {
            @Override
            public void setupOutput(JobConf conf, Class<?> ignore) {
                conf.setOutputFormat(TextOutputFormat.class);
                conf.setOutputKeyClass(String.class);
            }
            
            @Override
            public void setupInput(JobConf conf, Class<?> ignore) {
                conf.setInputFormat(TextInputFormat.class);                
            }
        },
        JSON_FORMAT {            
            @Override
            public void setupOutput(JobConf conf, Class<?> ignore) {
                conf.setOutputFormat(TextOutputFormat.class);                
                conf.setOutputKeyClass(String.class);
            }
            
            @Override
            public void setupInput(JobConf conf, Class<?> ignroe) {
                conf.setInputFormat(TextInputFormat.class);                
            }
        },
        AVRO_FORMAT {
            @Override
            public void setupOutput(JobConf conf, Class<?> ignore) {
                conf.setOutputFormat(AvroOutputFormat.class);
                conf.setOutputKeyClass(AvroWrapper.class);
            }

            @Override
            public void setupInput(JobConf conf, Class<?> ignore) {
                conf.setInputFormat(AvroInputFormat.class);        
            }
        },
        PROTOBUF_FORMAT {
            @Override
            @SuppressWarnings("unchecked")
            public void setupOutput(JobConf conf, Class<?> protoClass) {
                setupOutputImpl(conf, protoClass);
            }
            @Override
            public void setupInput(JobConf conf, Class<?> protoClass) {
                setupInputImpl(conf, protoClass);
            }
            
            private <M extends Message> void setupOutputImpl(JobConf conf, Class<?> protoClass) {
                conf.setOutputFormat((Class<? extends OutputFormat>)
                    LzoProtobufB64LineOutputFormat.getOutputFormatClass((Class<M>) protoClass, conf));
            }

            private <M extends Message> void setupInputImpl(JobConf conf, Class<?> protoClass) {
                conf.setInputFormat((Class<? extends InputFormat>)
                    LzoProtobufB64LineInputFormat.getInputFormatClass((Class<M>) protoClass, conf));
            }
        };

        public abstract void setupOutput(JobConf conf, Class<?> protoClass);

        public abstract void setupInput(JobConf conf, Class<?> protoClass);
    }

    @Deprecated
    public Pipe(T prototype) {
        this.prototype = prototype;
    }

    public Pipe(String path) {
        this.path = path;
    }

    public boolean exists(Configuration conf) {
        Path dfsPath = new Path(path);
        try {
            FileSystem fs = dfsPath.getFileSystem(conf); 
            return fs.exists(dfsPath);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isObsolete(Configuration conf) {
        Path dfsPath = new Path(path);
        try {
            FileSystem fs = dfsPath.getFileSystem(conf); 
            // this needs to be smart - we should encode in the file metadata the dependents and their dates used
            // so we can verify that any existing antecedent is not newer and declare victory...
            if (fs.exists(dfsPath)) {
                FileStatus[] statuses = fs.listStatus(dfsPath);
                for (FileStatus status : statuses) {
                    if (!status.isDir()) {
                        if (format!=Formats.AVRO_FORMAT || status.getPath().toString().endsWith(".avro")) {
                            return false; // may check for extension for other types
                        }
                    } else {
                        if (!status.getPath().toString().endsWith("/_logs") && !status.getPath().toString().endsWith("/_temporary")) {
                            return false;
                        }
                    }
                }
            }
            return true; // needs more work!
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Phase getProducer() {
        return producer;
    }

    public void setProducer(Phase producer) {
        this.producer = producer;
    }

    public String getPath() {
        return path;
    }

    public Pipe<T> at(String path) {
        this.path = path;
        return this;
    }

    @Override
    public String toString() {
        return path + ":" + super.toString();
    }

    public void clearAndPrepareOutput(Configuration conf) {
        try {
            Path dfsPath = new Path(path);
            FileSystem fs = dfsPath.getFileSystem(conf);
            if (fs.exists(dfsPath)) {
                FileStatus[] statuses = fs.listStatus(dfsPath);
                for (FileStatus status : statuses) {
                    if (status.isDir()) {
                        if (!status.getPath().toString().endsWith("/_logs") && !status.getPath().toString().endsWith("/_temporary")) {
                            throw new IllegalArgumentException("Trying to overwrite directory with child directories: " + path);
                        }
                    }
                }
            } else {            
                fs.mkdirs(dfsPath);
            }
            fs.delete(dfsPath, true);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Pipe<T> of(Class<? extends T> ofClass) {
        if(Message.class.isAssignableFrom(ofClass)) {
            Pipe<T> pipe = new Pipe<T>((T)null);
            pipe.protoClass = ofClass;
            return pipe;
        }
            
        try {
            return new Pipe<T>(ofClass.newInstance());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Pipe<T> of(T prototype) {
        return new Pipe<T>(prototype);
    }

    public T getPrototype() {
        return prototype;
    }
    
    public Class<?> getPrototypeClass() {
        return protoClass;
    }
    
    void setPrototype(T prototype) {
        this.prototype = prototype;
    }

    public void delete(JobConf conf) {
        clearAndPrepareOutput(conf);
    }

    public Pipe stringFormat() {
        this.format = Formats.STRING_FORMAT;
        this.prototype = (T)new String();
        return this;
    }

    public Pipe jsonFormat() {
        this.format = Formats.JSON_FORMAT;
        return this;
    }

    public Pipe avroFormat() {
        this.format = Formats.AVRO_FORMAT;
        return this;
    }
    
    public <M extends Message> Pipe protoFormat() {
        this.format = Formats.PROTOBUF_FORMAT;
        return this;
    }
    
    public void setupOutput(JobConf conf) {
        format.setupOutput(conf, protoClass != null ? protoClass : prototype == null ? null : prototype.getClass());        
    }

    public long getTimestamp(JobConf conf) {
        try {
            Path dfsPath = new Path(path);
            FileSystem fs = dfsPath.getFileSystem(conf);
            return fs.getFileStatus(dfsPath).getModificationTime();            
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setupInput(JobConf conf) {
        format.setupInput(conf, protoClass != null ? protoClass : prototype == null ? null : prototype.getClass());        
    }
    
    // files at the same location are deemed equal, however
    // ColPipe needs to warn if there are inconsistencies

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Pipe other = (Pipe) obj;
        if (path == null) {
            if (other.path != null)
                return false;
        }
        else if (!path.equals(other.path))
            return false;
        return true;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((path == null) ? 0x123c67ce : path.hashCode());
        return result;
    }
}
