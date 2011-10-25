package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import com.twitter.elephantbird.mapreduce.io.BinaryBlockWriter;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A writer for LZO-encoded blocks of protobuf or Thrift objects, generally read by
 * a ProtobufBlockWriter or similar.
 */
public class LzoBinaryBlockRecordWriter<M, W extends BinaryWritable<M>>
    implements RecordWriter<M, W> {

  private BinaryBlockWriter<M> writer_;

  public LzoBinaryBlockRecordWriter(BinaryBlockWriter<M> writer) {
    writer_ = writer;
  }

  public void write(M nullWritable, W protoWritable)
      throws IOException {
    writer_.write(protoWritable.get());
    // the counters are not accessible
  }

  @Override
  public void close(Reporter reporter)
      throws IOException {
    writer_.finish();
    writer_.close();
  }
}
