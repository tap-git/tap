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
 * Copyright 2011 Think Big Analytics. All Rights Reserved.
 */
package tap;

import tap.compression.Compressions;
import tap.core.InfeasiblePlanException;
import tap.core.TapContext;
import tap.core.io.BinaryKey;
import tap.core.mapreduce.input.TapfileRecordReader;
import tap.core.mapreduce.io.BinaryWritable;
import tap.formats.*;
import tap.util.ObjectFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class Pipe<T> implements Iterable<T>, Iterator<T> {

    private TapContext<T> context; // for OutPipe
    private Iterator<AvroValue<T>> values; // for InPipe
    Formats format = Formats.UNKNOWN_FORMAT;
    private Phase producer;
    protected String path;
    protected T prototype;
    String uncompressedPath;
    protected Compressions compression = null;
    protected boolean isCompressed = false;
    boolean isTempfile = false;
    private DFSStat stat;
    
    //to support subscribe API
    boolean isDirectFileAccess = false;
    TapfileRecordReader recordReader = null;
	BinaryKey binaryKey = null; 
    BinaryWritable writable = null; 

    
    // Pipe's reference to the job configuration
    Configuration conf = null;

	public static <T> Pipe<T> of(Class<? extends T> ofClass) {
        try {
            return new Pipe<T>(ObjectFactory.newInstance(ofClass));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Pipe<T> of(T prototype) {
        return new Pipe<T>(prototype);
    }

    @Deprecated
	public Pipe(T prototype) {
        this.setPrototype(prototype);
    }

    Pipe(String path) {
        setPath(path);
        init();
    }
    
    /**
     * Temporary inter-phase pipe
     * @param isTemporary
     */
    Pipe(boolean isTemporary) {
    	isTempfile = isTemporary;
    }
    
    /**
     * Setup job configuration (and cache it) on the Pipe
     * @param conf
     */
    void setConf(Configuration conf) {
    	if (null == conf) {
    		throw new IllegalArgumentException("Please don't give us a null Configuration object");
    	}
    	this.conf = conf;
    }
    
   
    
    /**
     * 
     * @return The job configuration
     */
    Configuration getConf() {
    	return conf;
    }

    /**
     * Generate and return DFS file stat info
     * @return The file status
     */
    DFSStat stat() {
    	if (null == stat) {
    		this.stat = new DFSStat(path,getConf());
    	}
    	return stat;
    }
     
    /*
     * Probe HDFS to determine if this.path exists.
     */
    boolean exists() {
    	 return stat().exists;
    }

    /**
     * Determine if file(s) in path are obsolete. Used in generating a work
     * plan.
     * @return True if obsolete
     */
	boolean isObsolete() {
		// this needs to be smart - we should encode in the file metadata
		// the dependents and their dates used
		// so we can verify that any existing antecedent is not newer and
		// declare victory...
		if (stat().exists) {
			for (FileStatus status : stat().getStatuses()) {
				if (!status.isDir()) {
					// TODO add other types?
					if (getFormat() != Formats.AVRO_FORMAT
							|| status.getPath().toString().endsWith(".avro")) {
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

	}
	
	//for subscribe
	
	public void setRecordReader(TapfileRecordReader reader)
	{
		this.recordReader = reader;
		isDirectFileAccess = true;
		binaryKey = reader.createKey();
        writable = reader.createValue();
    
	}

    /**
     * Check outputs
     * @param conf
     */
    void clearAndPrepareOutput() {
        try {
            if (stat().exists) {
                for (FileStatus status : stat().getStatuses()) {
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
                stat().fs.mkdirs(stat().dfsPath);
            }
            stat().fs.delete(stat().dfsPath, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

	/**
	 * Make determination of this (input) pipe is valid. This test is only
	 * useful during early binding.
	 * 
	 * @return
	 */
	boolean isValidInput() {
		return isTempfile 
				|| hasWildcard()
				|| isFile()
				|| isSingleDir()
				;
	}
	
	boolean isFile() {
		return stat().exists && stat().isFile;
	}
	
	boolean isSingleDir() {
		return stat().exists && !stat().isFile && !hasSubdirs();
	}

	boolean hasSubdirs() {
		return false; //TODO: Implement logic here if performance is acceptable
	}

	boolean hasWildcard() {
		return path.contains("*") 
				|| path.contains("?")
				|| path.contains("[");
	}
	

	
    public void delete() {
        clearAndPrepareOutput();
    }

    @SuppressWarnings("unchecked")
    public Pipe<T> stringFormat() {
        setPrototype((T) new String());
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

    // Compression Methods
    public Pipe<T> gzipCompression() {
        this.setCompression(Compressions.GZIP_COMPRESSION);
        return this;
    }

    public Compressions getCompression() {
        return compression;
    }

    void setupOutput(JobConf conf) {
        getFormat().getFileFormat().setupOutput(conf,
                getPrototype() == null ? null : getPrototype().getClass());
        if (this.isCompressed == true) {
            getCompression().getCompression().setupOutput(conf);
        }

    }

    public void setupInput(JobConf conf) {
        getFormat().getFileFormat().setupInput(conf,
                getPrototype() == null ? null : getPrototype().getClass());
        if (this.isCompressed == true) {
            getCompression().getCompression().setupInput(conf);
        }

    }

    /**
     * Return timestamp of @path
     * 
     * @param conf
     *            Environment configuration
     * @return the timestamp
     */
    public long getTimestamp() {
    	return stat().timestamp;
    }

    /**
     * Files at the same location are deemed equal, however Pipe needs to warn
     * if there are inconsistencies.
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

    /**
     * Turn on/off the Pipe's compression
     * 
     * @param isCompressed
     *            true if compression is to be used
     * @return this
     */
    public Pipe<T> compressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
        return this;
    }

    /**
     * InPipe type constructor Reducer In pipe
     * 
     * @param values
     */
    public Pipe(Iterator<AvroValue<T>> values) {
        this.values = values;
    }

	public boolean hasNext() {
		if(isDirectFileAccess)
		{
			return recordReader.hasNext();
		}
		else
		{
			return this.values.hasNext();
		}
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
     * 
     * @return Object value
     */
    public T next() {
    	if(isDirectFileAccess)
    	{
    		try {
				recordReader.next(binaryKey, writable);
				return (T) writable.get();  //is there anyway to get the type?
			}
    		catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
				return (T) null;
			}
    	}
    	else
    	{
    		T val = this.values.next().datum();
    		return (T) val;
    	}
		
    }

    /**
     * Alias for next()
     * 
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
     * @param context
     *            The context to set
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
    
    public void put(T out, String multiName) {
        this.context.write(out, multiName);
    }

    /**
     * @return The phase that produces this file.
     */
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
        setPath(path);
        return this;
    }

    @Override
    public String toString() {
        return path + ":" + super.toString();
    }

    public T getPrototype() {
        return prototype;
    }
    
    public Formats getPipeFormat() throws FileNotFoundException, IOException, IllegalArgumentException
    {
    	if(!isFile())
    		throw new IllegalArgumentException("Pipe is not associated with a file, so has no format");
    	Path p = new Path(path);
    	
    	return sniffFileFormat(p); 
    }
    
    public Class  readPipeClassFromFile(Configuration job) throws IOException {
    	
    	if(path == null)
    		throw new IllegalArgumentException("specify file or directory for mapper before setting prototype");
    
    	Path p = new Path(path);
    	
    	return TapfileRecordReader.readMessageClass(job, p);
    }
    	
    
    public void setPrototypeForMapperInput(T prototype) throws  InfeasiblePlanException {
    	Formats sniffedFileFormat;
    	
    	if(path == null)
    		throw new IllegalArgumentException("specify file or directory for mapper before setting prototype");
    
    	Path p = new Path(path);
    	
    	if(isSingleDir())
    	{
    		FileStatus[] status;
    		try {
    			FileSystem f = FileSystem.get(getConf());
    			 status = f.listStatus(p);
    		}
    		catch(IOException ioexception)
    		{
    			throw new InfeasiblePlanException("invalid path");
    		}
    				
			if(status.length == 0)  //directory is empty
    		{	this.prototype = prototype;
    			return;
    		}
    		else //read the first file in directory
    		{
    			p = status[0].getPath();
    			if(p.getName().equals("_SUCCESS"))
    			{
    				if(status.length == 1)  //directory is empty except for _SUCCESS file
    				{
    					this.prototype = prototype;
    					return;
    				}
    				else
    				{
    					p = status[1].getPath();
    				}
    			}
    		}
			
    	}
    	
    	try {
    		sniffedFileFormat = sniffFileFormat(p);
    	}
    	
    	catch(Exception e)
    	{
    		throw new InfeasiblePlanException(e.getMessage());
    	}
    	
    	if(format != sniffedFileFormat) 
    	{
    		//alert user?
    		//override file extension
    		format = sniffedFileFormat;
    	}
    	
    	
    	
    	if(format.getFileFormat().instanceOfCheck(prototype))
		{
			//can do additional checking i.e., read file to make sure it matches it file extension, make sure it contains the correct objects.
			this.prototype = prototype;
		}
		else
		{
			
			throw new InfeasiblePlanException("Pipe prototype and file type are not compatible");
		}
    	
    			
    	
    	
    }
    
    //NB this duplicates code found in TapfileRecodReader.  Need to re-factor.
    
    private Formats sniffFileFormat(Path path) throws IOException, FileNotFoundException {

    	byte[] header;
    	
    	
    	FileSystem fs = path.getFileSystem(this.getConf());
    	
    	FSDataInputStream in = null;
    	try {
    		in = fs.open(path);
    		header = new byte[1000];
    		in.read(header);
    		in.close();
    		
    	} finally {
    		if(in != null)
    			in.close();
    	}
    	return determineFileFormat(header);
    	
}


private Formats determineFileFormat(byte[] header) {
    for (Formats format : Formats.values()) {
        if (format.getFileFormat().signature(header)) {
            return format;

        }
    }
    return Formats.UNKNOWN_FORMAT;
}


    public void setPrototype(T prototype) {
    	
    	if (prototype == null) {
    		return;
    	}
    	
    	this.prototype = prototype;
        init();
    }
    
    public Formats getFormat() {
        return format;
    }

    public void setFormat(Formats format) {
        this.format = format;
    }

    public String getUncompressedPath() {
        return uncompressedPath;
    }

    protected void setUncompressedPath(String uncompressedPath) {
        this.uncompressedPath = uncompressedPath;
    }

    protected void setPath(String path) {
        this.path = path;
    }

    protected Formats determineFormat() {

  
    	
        for (Formats f : Formats.values()) {
            FileFormat fileFormat = f.getFileFormat();
            if (fileFormat.matches(this)) {
                return f;
            }
        }
        
        return Formats.UNKNOWN_FORMAT;
        
    
    }

    /**
     * Based on path name, determine if file is compressed
     */
    protected void determineCompression() {
        if (null != path) {
            if (this.path.endsWith(Compressions.GZIP_COMPRESSION
                    .fileExtension())) {
                this.isCompressed = true;
                setCompression(Compressions.GZIP_COMPRESSION);
                this.setUncompressedPath(this.path.replaceAll(".gz$", ""));
            } else {
                this.setUncompressedPath(path);
            }
        }
    }

    public Pipe<T> setCompression(Compressions compression) {
        this.isCompressed = true;
        this.compression = compression;
        return this;

    }

    public boolean isCompressed() {
        return isCompressed;
    }

    protected void init() {
        determineCompression();
        Formats format = determineFormat();
        setFormat(format);
        
    }
}
