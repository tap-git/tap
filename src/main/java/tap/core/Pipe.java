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

import tap.compression.Compressions;
import tap.formats.*;
import tap.util.ObjectFactory;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class Pipe<T> implements Iterable<T>, Iterator<T> {

    private TapContext<T> context; // for OutPipe
    private Iterator<AvroValue<T>> values;    // for InPipe
	private Phase producer;
	private String path;
	private T prototype;
	private Formats format = Formats.AVRO_FORMAT;
	private boolean isCompressed = false;
	private String uncompressedPath;
	private Compressions compression = null;

	@Deprecated
	public Pipe(T prototype) {
		this.prototype = prototype;
	}

	public Pipe(String path) {
	    setPath(path);
	}

    private void setPath(String path) {
		this.path = path;

		determineCompression();
		determineFormat();
	}

	private void determineCompression() {
        if (this.path.endsWith(Compressions.GZIP_COMPRESSION.fileExtension())) {
            this.isCompressed = true;
            setCompression(Compressions.GZIP_COMPRESSION);
            this.uncompressedPath = this.path.replaceAll(".gz$", "");
        } else {
            this.uncompressedPath = path;
        }
	}

	/*
	 * determine pipe's format based on file extension and configure pipe
	 * automatically
	 */
	private void determineFormat() {
		for (Formats f : Formats.values()) {
			if (f.getFileFormat().matches(this.uncompressedPath)) {
				f.getFileFormat().setPipeFormat(this);
			}
		}
		if (this.getFormat().equals(Formats.UNKNOWN_FORMAT)) {
			// open file, read first couple lines
		}
	}
	
	/*
	 * Probe HDFS to determine if this.path exists.
	 */
	public boolean exists(Configuration conf) {
		Path dfsPath = new Path(path);
		try {
			FileSystem fs = dfsPath.getFileSystem(conf);
			return fs.exists(dfsPath);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Determine if file(s) in path are obsolete. Used in generating a work plan.
	 * @param conf Job configuration
	 * @return True if obsolete
	 */
	public boolean isObsolete(Configuration conf) {
		Path dfsPath = new Path(path);
		try {
			FileSystem fs = dfsPath.getFileSystem(conf);
			// this needs to be smart - we should encode in the file metadata
			// the dependents and their dates used
			// so we can verify that any existing antecedent is not newer and
			// declare victory...
			if (fs.exists(dfsPath)) {
				FileStatus[] statuses = fs.listStatus(dfsPath);
				for (FileStatus status : statuses) {
					if (!status.isDir()) {
						// TODO add other types?
						if (getFormat() != Formats.AVRO_FORMAT
								|| status.getPath().toString()
										.endsWith(".avro")) {
							return false; // may check for extension for other
											// types
						}
					} else {
						if (!status.getPath().toString().endsWith("/_logs")
								&& !status.getPath().toString()
										.endsWith("/_temporary")) {
							return false;
						}
					}
				}
			}
			return true; // needs more work!
		} catch (IOException e) {
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

	/*
	 * Set pipe's path.
	 */
	public Pipe<T> at(String path) {
	    setPath(path);
		return this;
	}

	@Override
	public String toString() {
		return path + ":" + super.toString();
	}

	/**
	 * 
	 * @param conf
	 */
	protected void clearAndPrepareOutput(Configuration conf) {
		try {
			Path dfsPath = new Path(path);
			FileSystem fs = dfsPath.getFileSystem(conf);
			if (fs.exists(dfsPath)) {
				FileStatus[] statuses = fs.listStatus(dfsPath);
				for (FileStatus status : statuses) {
					if (status.isDir()) {
						if (!status.getPath().toString().endsWith("/_logs")
								&& !status.getPath().toString()
										.endsWith("/_temporary")) {
							throw new IllegalArgumentException(
									"Trying to overwrite directory with child directories: "
											+ path);
						}
					}
				}
			} else {
				fs.mkdirs(dfsPath);
			}
			fs.delete(dfsPath, true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> Pipe<T> of(Class<? extends T> ofClass) {
        try {
            return new Pipe<T>(ObjectFactory.newInstance(ofClass));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

	/*
	 * Construct new Pipe<T> and set pipe's prototype.
	 */
	public static <T> Pipe<T> of(T prototype) {
		return new Pipe<T>(prototype);
	}

	public T getPrototype() {
		return prototype;
	}

	void setPrototype(T prototype) {
		this.prototype = prototype;
	}

	public void delete(JobConf conf) {
		clearAndPrepareOutput(conf);
	}

	@SuppressWarnings("unchecked")
    public Pipe<T> stringFormat() {
		this.setFormat(Formats.STRING_FORMAT);
		this.prototype = (T) new String();
		return this;
	}

	public Pipe<T> jsonFormat() {
		this.setFormat(Formats.JSON_FORMAT);
		return this;
	}

	public Pipe<T> avroFormat() {
		this.setFormat(Formats.AVRO_FORMAT);
		return this;
	}
    
	public Pipe<T> protoFormat() {
        this.setFormat(Formats.TAPPROTO_FORMAT);
        return this;
    }
	
    //Compression Methods
    public Pipe<T> gzipCompression() {
    	this.setCompression(Compressions.GZIP_COMPRESSION);
    	return this;
    }

    public Pipe<T> setCompression(Compressions compression) {
    	this.isCompressed = true;
    	this.compression = compression;
    	return this;
    	
    }
    
    public Compressions getCompression() {
    	return this.compression;
    }	
	
    public void setupOutput(JobConf conf) {
        getFormat().getFileFormat().setupOutput(conf,
                prototype == null ? null : prototype.getClass());
        if(this.isCompressed == true) {
        	getCompression().getCompression().setupOutput(conf);
        }
        
    }

    public void setupInput(JobConf conf) {
        getFormat().getFileFormat().setupInput(conf, prototype == null ? null : prototype.getClass());
        if (this.isCompressed == true) {
        	getCompression().getCompression().setupInput(conf);
        }
        
    }
    
	/**
	 * Return timestamp of @path
	 * @param conf Environment configuration
	 * @return the timestamp
	 */
	public long getTimestamp(JobConf conf) {
		try {
			Path dfsPath = new Path(path);
			FileSystem fs = dfsPath.getFileSystem(conf);
			return fs.getFileStatus(dfsPath).getModificationTime();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Files at the same location are deemed equal, however
	 * Pipe needs to warn if there are inconsistencies.
	 */
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
		} else if (!path.equals(other.path))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((path == null) ? 0x123c67ce : path.hashCode());
		return result;
	}

	public Formats getFormat() {
		return format;
	}

	public void setFormat(Formats format) {
		this.format = format;
	}

    public boolean isCompressed() {
        return isCompressed;
    }

    /**
     * Turn on/off the Pipe's compression
     * @param isCompressed true if compression is to be used
     * @return this
     */
    public Pipe<T> compressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
        return this;
    }
    
    /**
     * InPipe type constructor
     * Reducer In pipe
     * @param values
     */
    public Pipe(Iterator<AvroValue<T>> values) {
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
        T val = this.values.next().datum();
        return (T) val;
    }
    
    /**
     * Alias for next()
     * @return The next value in the Iterator
     */
    public T get() {
        return this.next();
    }
    
    /**
     * @return the context
     */
    public TapContext<T> getContext() {
        return context;
    }

    /**
     * @param context The context to set
     */
    public void setContext(TapContext<T> context) {
        this.context = context;
    }

    /**
     * Put value @out into output
     * 
     * @param out
     *            The value to put
     */
    public void put(T out) {
        this.context.write(out);
    }
}
