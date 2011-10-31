package tap.formats.tapproto;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;

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
        
        int limit = stream.pushLimit(stream.readRawVarint32());
        Tapfile.Header trailer = Tapfile.Header.parseFrom(stream);
        stream.popLimit(limit);
        
        Assert.assertEquals("test.file", trailer.getInitialPipeName());
        Assert.assertEquals("TestMsg", trailer.getMessageName());
        Assert.assertEquals(5000, trailer.getMessageCount());
        
        System.out.println(trailer);
        
        // read index
        channel.position(trailer.getIndexOffset());
        CodedInputStream idx = CodedInputStream.newInstance(fin);
        bytes = idx.readRawBytes(8);
        Assert.assertEquals("upixnone",  new String(bytes));
        
        FileInputStream dataInputStream = new FileInputStream(file);
        FileChannel dataChannel = dataInputStream.getChannel();
        
        // read data
        long dataBlockCount = trailer.getDataBlockCount();
        int totalMessagesRead = 0;
        while(dataBlockCount-- > 0) {
            // read next index entry
            limit = idx.pushLimit(idx.readRawVarint32());
            Tapfile.IndexEntry entry = Tapfile.IndexEntry.parseFrom(idx);
            idx.popLimit(limit);
            System.out.println(entry);

            int messageCount = entry.getMessageCount();
            // read data block
            dataChannel.position(entry.getDataOffset());
            dataInputStream.read(bytes);
            Assert.assertEquals("datagzip", new String(bytes));
            GZIPInputStream gzipStream = new GZIPInputStream(dataInputStream);
            CodedInputStream dataStream = CodedInputStream.newInstance(gzipStream);
            
            while(messageCount-- > 0) {
                int size = dataStream.readRawVarint32();
                byte[] keyBytes = dataStream.readRawBytes(size);
                limit = dataStream.pushLimit(dataStream.readRawVarint32());
                Testmsg.TestMsg msg = Testmsg.TestMsg.parseFrom(dataStream);
                dataStream.popLimit(limit);
                totalMessagesRead += 1;
            }
        }
        
        fin.close();
        dataInputStream.close();
        
        Assert.assertEquals(5000, totalMessagesRead);
    }
}
