package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.util.LzoUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Base class for Lzo outputformats.
 * provides an helper method to create lzo output stream.
 */
public abstract class LzoOutputFormat<K, V> extends FileOutputFormat<K, V> {

  public static final Logger LOG = LogManager.getLogger(LzoOutputFormat.class);

  /**
   * Helper method to create lzo output file needed to create RecordWriter
   */
  protected DataOutputStream getOutputStream(JobConf conf, String name)
                  throws IOException {
      // LzopCodec codec = new LzopCodec();
      // Path path = getPathForCustomFile(conf, name + codec.getDefaultExtension()); 
      // return LzoUtils.getIndexedLzoOutputStream(conf, path);
    
      // Path file = getPathForCustomFile(conf, name + ".protobuf"); 
      Path file = getTaskOutputPath(conf, name);
      FileSystem fs = file.getFileSystem(conf);
      return fs.create(file, false);
  }
}
