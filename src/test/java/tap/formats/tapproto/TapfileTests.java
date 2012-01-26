package tap.formats.tapproto;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import junit.framework.Assert;

import org.junit.Test;

import tap.formats.tapproto.Tapfile.IndexEntry;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.FileDescriptor;

public class TapfileTests {
    
    static final int DEFAULT_TARGET_BLOCK_SIZE  = 256 * 1024;
    static final String ASCII = "ASCII";
    static final int ALIGNMENT = 512;
    static final byte[] PAD_BYTES = new byte[ALIGNMENT];
    
    static {
        Arrays.fill(PAD_BYTES, (byte) 0xFA);
    }

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
    
    static void writeRawBytes(CodedOutputStream stream, String data) throws Exception {
        stream.writeRawBytes(data.getBytes(ASCII));
    }
    
    static void pad(CountingOutputStream out, int remnant) throws Exception {
        long padBytes = ALIGNMENT - 1 - ((out.getCount() + ALIGNMENT-1) % ALIGNMENT);
        padBytes -= remnant;
        out.write(PAD_BYTES, 0, (int) padBytes);
    }
    
    public void testCanWriteFile() throws Exception {
        Tapfile.Header.Builder headerBuilder = Tapfile.Header.newBuilder(); 
        headerBuilder.setInitialPipeName("test.file");
        headerBuilder.setKeyDescriptor("<NEEDS CODING>");
        
        CountingOutputStream out = new CountingOutputStream(
                new FileOutputStream("share/test.tapfile.out"));
        
        CodedOutputStream fileStream = CodedOutputStream.newInstance(out);
        
        headerBuilder.setMessageName(Testmsg.TestMsg.class.getName());
        headerBuilder.setTargetDecompSize(DEFAULT_TARGET_BLOCK_SIZE);
        
        writeRawBytes(fileStream, "tapproto");
        writeRawBytes(fileStream, "head");
        writeRawBytes(fileStream, "none");
        
        Tapfile.Header header = headerBuilder.build();
        fileStream.writeRawVarint32(header.getSerializedSize());
        header.writeTo(fileStream);
        fileStream.flush();
        pad(out, 0);
        
        Tapfile.Header.Builder trailerBuilder = Tapfile.Header.newBuilder().mergeFrom(header);
        
        // add descriptors from imported .proto Files to the trailer
        for(FileDescriptor fd : Testmsg.TestMsg.getDescriptor().getFile().getDependencies()) {
            trailerBuilder.addFormatDescriptor(fd.toProto().toByteString());
        }
        
        // add the main File descriptor proto to the trailer
        FileDescriptor fdmain = Testmsg.TestMsg.getDescriptor().getFile();
        trailerBuilder.addFormatDescriptor(fdmain.toProto().toByteString());
        
        // index stream
        ByteArrayOutputStream idx = new ByteArrayOutputStream(1024);
        CodedOutputStream idxStream = CodedOutputStream.newInstance(idx);
        writeRawBytes(idxStream, "upix");
        writeRawBytes(idxStream, "none");

        // data
        IndexEntry.Builder entryBuilder = IndexEntry.newBuilder();
        GZIPOutputStream gzipStream = new GZIPOutputStream(out);
        CountingOutputStream countingDataStream = new CountingOutputStream(gzipStream, out.getCount());
        CodedOutputStream dataStream = CodedOutputStream.newInstance(countingDataStream);
        
        byte[] key = new String("hello").getBytes("ASCII");
        List<Testmsg.TestMsg> messages = new ArrayList<Testmsg.TestMsg>();
        
        entryBuilder.setFirstKey(ByteString.copyFrom(key));
        entryBuilder.setDataOffset(out.getCount());
        entryBuilder.setMessageCount(0);
        writeRawBytes(fileStream, "data");
        writeRawBytes(fileStream, "gzip");
        
        for(Testmsg.TestMsg msg : messages) {
            dataStream.writeRawVarint32(key.length);
            dataStream.writeRawBytes(key);
            dataStream.writeRawVarint32(msg.getSerializedSize());
            msg.writeTo(dataStream);
            entryBuilder.setMessageCount(entryBuilder.getMessageCount() + 1);
            trailerBuilder.setLastKey(ByteString.copyFrom(key));
            dataStream.flush();
            
            if(countingDataStream.getCount() >= DEFAULT_TARGET_BLOCK_SIZE) {
                flush(trailerBuilder, entryBuilder, countingDataStream.getCount());
                
                // these go into flush()
                gzipStream.finish();
                pad(countingDataStream, 0);
                entryBuilder.setDataBytes(countingDataStream.getCount() - entryBuilder.getDataOffset());
                IndexEntry entry = entryBuilder.build();
                idxStream.writeRawVarint32(entry.getSerializedSize());
                entry.writeTo(idxStream);
                
                // reinitialize entryBuilder
            }
            dataStream.flush(); // TODO: keep track of currentCount
            
            // flush()
            
            idxStream.flush();
            idx.close();
            byte[] indexBytes = idx.toByteArray();
            trailerBuilder.setIndexOffset(countingDataStream.getCount() /* currentCount */);
            dataStream.writeRawBytes(indexBytes);
            dataStream.flush();
            pad(countingDataStream, 0);
            trailerBuilder.setIndexBytes(countingDataStream.getCount() - trailerBuilder.getIndexOffset());
           
            Tapfile.Header trailer = trailerBuilder.build();
            long trailerOffset = countingDataStream.getCount();
            writeRawBytes(dataStream, "trai");
            writeRawBytes(dataStream, "none");
            dataStream.writeRawVarint32(trailer.getSerializedSize());
            trailer.writeTo(dataStream);
            dataStream.flush();
            pad(countingDataStream, 16);
            
            dataStream.writeRawLittleEndian64(trailerOffset);
            writeRawBytes(dataStream, "tapproto");
            dataStream.flush();
            countingDataStream.close();
        }
        
        out.close();
    }
    
    static void flush(
            Tapfile.Header.Builder trailerBuilder,
            Tapfile.IndexEntry.Builder indexEntryBuilder,
            long dataByteCount) {
        trailerBuilder.setDataBlockCount(trailerBuilder.getDataBlockCount() + 1);
        trailerBuilder.setMessageCount(trailerBuilder.getMessageCount() + indexEntryBuilder.getMessageCount());
        trailerBuilder.setUncompressedBytes(trailerBuilder.getUncompressedBytes() + dataByteCount);
        if(trailerBuilder.getMaxDecompSize() < dataByteCount)
            trailerBuilder.setMaxDecompSize(dataByteCount);
    }
    
    /** Same as Guava CountingOutputStream but with support for an initial offset **/
    class CountingOutputStream extends FilterOutputStream {
        private long offset;
        private long count;

        public CountingOutputStream(OutputStream out, long offset) {
            super(out);
            this.offset = offset;
        }
        
        public CountingOutputStream(OutputStream out)
        {
            this(out, 0);
        }

        /** Returns the number of bytes written. */
        public long getCount() {
            return offset + count;
        }

        @Override public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            count += len;
        }
        
        @Override public void write(int b) throws IOException {
            out.write(b);
            count++;
        }
    }
}
