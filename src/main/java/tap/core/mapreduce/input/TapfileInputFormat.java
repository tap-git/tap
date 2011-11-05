package tap.core.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import tap.core.mapreduce.io.BinaryWritable;
import tap.util.Protobufs;
import tap.util.TypeRef;

import com.google.protobuf.Message;

public class TapfileInputFormat<M extends Message> extends FileInputFormat<LongWritable, BinaryWritable<M>>{
    private TypeRef<M> typeRef;

    @SuppressWarnings("rawtypes")
    public static <M extends Message> Class<TapfileInputFormat>
       getInputFormatClass(Class<M> protoClass, Configuration job) {
      Protobufs.setClassConf(job, TapfileInputFormat.class, protoClass);
      return TapfileInputFormat.class;
    }

    @SuppressWarnings("deprecation")
    @Override
    public RecordReader<LongWritable, BinaryWritable<M>> getRecordReader(
            InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        if (typeRef == null) {
            typeRef = Protobufs.getTypeRef(job, TapfileInputFormat.class);
        }
        return new TapfileRecordReader<M>(job, (FileSplit) genericSplit, typeRef);
    }
}
