package tap.core.mapreduce.input;

import java.io.File;

import junit.framework.Assert;
import tap.core.mapreduce.io.BinaryWritable;
import tap.formats.tapproto.Testmsg.TestMsg;
import tap.util.TypeRef;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

public class TapfileRecordReaderTests {
    TypeRef<TestMsg> typeRef = new TypeRef<TestMsg>(TestMsg.class) {};
    
    @Test
    public void testCanReadFile() throws Exception {
        JobConf job = new JobConf();
        RawLocalFileSystem fs = new RawLocalFileSystem();
        Path path = new Path(fs.getWorkingDirectory(), "share/test.tapfile.bugfixed");
        
        TapfileRecordReader reader = new TapfileRecordReader<TestMsg>(job, path, typeRef);
        
        LongWritable key = reader.createKey();
        BinaryWritable<TestMsg> value = reader.createValue();
        
        long numberOfMessages = 0;
        
        while(reader.next(key, value)) {
            numberOfMessages += 1; 
        }
        
        Assert.assertEquals(5000, numberOfMessages);

        reader.close();
    }
}
