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
        File file = new File("share/test.tapfile");
        FileInputStream fin = new FileInputStream(file);
        
        FileChannel channel = fin.getChannel();
        
        channel.position(file.length() - 16);
        CodedInputStream stream = CodedInputStream.newInstance(fin);
        
        long trailerOffset = stream.readRawLittleEndian64();
        
        byte[] bytes = stream.readRawBytes(8);
        String sig = new String(bytes);
        System.out.println("file size=" + file.length());
        System.out.println("trailerOffset=" + trailerOffset);
        System.out.println(sig.equals("tapproto"));
        
        channel.position(trailerOffset);
        System.out.println("file pos=" + channel.position());
        bytes = stream.readRawBytes(4);
        System.out.println(new String(bytes)); // "trai"
        bytes = stream.readRawBytes(4);
        System.out.println(new String(bytes)); // "none"
        
        System.out.println("file pos=" + channel.position());
        /*
        Tapfile.Header.Builder builder = Tapfile.Header.newBuilder();
        stream.readMessage(builder, null);
        Tapfile.Header trailer = builder.build();
        */
        Tapfile.Header trailer = Tapfile.Header.newBuilder()
                .mergeFrom(stream)
                .build();
        System.out.println("file pos=" + channel.position());
        
        System.out.println("message name=" + trailer.getMessageName());
        System.out.println("message count=" + trailer.getMessageCount());
    }
}
