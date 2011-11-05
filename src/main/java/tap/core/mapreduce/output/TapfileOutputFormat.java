package tap.core.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import tap.core.mapreduce.io.BinaryWritable;
import tap.util.Protobufs;
import tap.util.TypeRef;

import com.google.protobuf.Message;

public class TapfileOutputFormat<M extends Message> extends FileOutputFormat<NullWritable, BinaryWritable<M>>{

    private TypeRef<M> typeRef;

    @SuppressWarnings("rawtypes")
    public static <M extends Message> Class<TapfileOutputFormat>
       getOutputFormatClass(Class<M> protoClass, Configuration job) {
      Protobufs.setClassConf(job, TapfileOutputFormat.class, protoClass);
      return TapfileOutputFormat.class;
    }

    @SuppressWarnings("deprecation")
    @Override
    public RecordWriter<NullWritable, BinaryWritable<M>> getRecordWriter(
            FileSystem fileSystem, JobConf job, String name, Progressable progressable)
            throws IOException {
        if (typeRef == null) {
            typeRef = Protobufs.getTypeRef(job, TapfileOutputFormat.class);
        }
        Path path = FileOutputFormat.getTaskOutputPath(job, name);
        return new TapfileRecordWriter<M>(job, path, progressable, typeRef);
    }
}
