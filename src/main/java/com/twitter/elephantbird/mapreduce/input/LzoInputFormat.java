package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.hadoop.compression.lzo.LzoIndex;
import com.twitter.elephantbird.mapreduce.output.LzoOutputFormat;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for lzop compressed files. This class
 * handles the nudging the input splits onto LZO boundaries using the existing LZO index files.
 * Subclass and implement getRecordReader to define custom LZO-based input formats.<p>
 * <b>Note:</b> unlike the stock FileInputFormat, this recursively examines directories for matching files.
 */
public abstract class LzoInputFormat<K, V> extends FileInputFormat<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoInputFormat.class);

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    for (FileStatus file : super.listStatus(job))
      if (file.getPath().getName().endsWith(LzoOutputFormat.EXT))
        result.add(file);
    return result.toArray(new FileStatus[0]);
  }
  
  @Override
    public RecordReader<K, V> getRecordReader(InputSplit inputSplit, JobConf job,
            Reporter reporter) throws IOException {
        // TODO Auto-generated method stub
      LzoRecordReader<K, V> reader = createRecordReader(job);
      reader.initialize(inputSplit, job);
      return reader;
    }
  
   protected abstract LzoRecordReader<K, V> createRecordReader(JobConf job) throws IOException;
}
