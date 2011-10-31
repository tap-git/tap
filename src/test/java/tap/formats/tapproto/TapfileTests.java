package tap.formats.tapproto;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;

import junit.framework.Assert;

import org.junit.Test;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;

public class TapfileTests {

    @Test
    public void testCanReadFile() throws Exception {
        File file = new File("share/test.tapfile.bugfixed");
        FileInputStream fin = new FileInputStream(file);
        
        FileChannel channel = fin.getChannel();
        
        channel.position(file.length() - 16);
        CodedInputStream stream = CodedInputStream.newInstance(fin);
        
        long trailerOffset = stream.readRawLittleEndian64();
        
        byte[] bytes = stream.readRawBytes(8);
        Assert.assertEquals("tapproto", new String(bytes));
        
        channel.position(trailerOffset);
        stream = CodedInputStream.newInstance(fin);
        bytes = stream.readRawBytes(8);
        Assert.assertEquals("trainone", new String(bytes));
        
        int limit = stream.readRawVarint32();
        
        stream.pushLimit(limit);
        Tapfile.Header trailer = Tapfile.Header.parseFrom(stream);
        
        Assert.assertEquals("test.file", trailer.getInitialPipeName());
        Assert.assertEquals("TestMsg", trailer.getMessageName());
        Assert.assertEquals(5000, trailer.getMessageCount());
        
        stream.popLimit(limit);
        
        fin.close();
    }
}
