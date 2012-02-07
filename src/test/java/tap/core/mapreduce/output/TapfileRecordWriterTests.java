package tap.core.mapreduce.output;

import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import tap.core.io.BinaryKey;
import tap.core.io.Bytes;
import tap.core.io.ReuseByteArrayOutputStream;
import tap.core.io.SortOrder;
import tap.core.mapreduce.input.TapfileRecordReader;
import tap.core.mapreduce.io.BinaryWritable;
import tap.core.mapreduce.io.ProtobufWritable;
import tap.formats.tapproto.Testmsg.TestMsg;
import tap.util.TypeRef;

public class TapfileRecordWriterTests {
    TypeRef<TestMsg> typeRef = new TypeRef<TestMsg>(TestMsg.class) {};
    
    @Test
    public void testCanWriteAndReadBack() throws Exception {
        File file = File.createTempFile("test_", ".tapproto");
        Path path = new Path("file:" + file.getAbsolutePath());
        
        try {
            writeOneToFiveThousand(path);
            
            TapfileRecordReader reader = new TapfileRecordReader<TestMsg>(new JobConf(), path, typeRef);
            
            BinaryKey binaryKey = reader.createKey();
            BinaryWritable<TestMsg> writable = reader.createValue();
            
            int count = 0;
            
            while(reader.next(binaryKey, writable)) {
                count += 1;
                
                String key = Bytes.toString(binaryKey.getBuffer(), 0, SortOrder.ASCENDING);
                TestMsg msg = writable.get(); 
                
                Assert.assertEquals(Integer.toString(count), key);
                Assert.assertEquals(count, msg.getSize());
                Assert.assertEquals("data_" + Integer.toString(count), msg.getData());
            }
            
            reader.close();
            
            Assert.assertEquals(5000, count);

        } finally {
            if(file.exists())
                file.delete();
        }
    }
    
    private void writeOneToFiveThousand(Path path) throws Exception {
        TapfileRecordWriter<TestMsg> writer = new TapfileRecordWriter<TestMsg>(new JobConf(), path, typeRef);
        
        ReuseByteArrayOutputStream out = new ReuseByteArrayOutputStream(256);
        
        BinaryKey key = new BinaryKey();
        BinaryWritable<TestMsg> writable = new ProtobufWritable<TestMsg>(typeRef);
        for(int i = 1; i <= 5000; ++i) {
        	out.reset();
        	Bytes.writeString(Integer.toString(i), out, SortOrder.ASCENDING);
        	key.set(out.getBuffer(), out.getCount());
        	
            TestMsg message = TestMsg.newBuilder()
                    .setSize(i)
                    .setData("data_" + Integer.toString(i))
                    .build();
            
            writable.set(message);
            writer.write(key, writable);
        }
        
        writer.close(null);
    }
}
