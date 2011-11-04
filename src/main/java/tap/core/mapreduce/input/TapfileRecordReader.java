package tap.core.mapreduce.input;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import tap.formats.tapproto.Tapfile;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

public class TapfileRecordReader<M extends Message> implements RecordReader<LongWritable, BinaryWritable<M>> {

    private FSDataInputStream inputStream;
    private long totalSize;
    private Tapfile.Header trailer;
    private List<Tapfile.IndexEntry> indexEntries = new ArrayList<Tapfile.IndexEntry>();
    private ProtobufConverter<M> converter;
    private LongWritable key;
    private ProtobufWritable<M> value;
    private int currentIndex = -1;
    private long messageCount;
    
    private CodedInputStream dataStream;
    
    public TapfileRecordReader(Configuration job, FileSplit split, TypeRef<M> typeRef) throws IOException {
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FileStatus status = fs.getFileStatus(file);
        
        initialize(fs.open(file), status.getLen(), typeRef);
    }
    
    public TapfileRecordReader(File file, TypeRef<M> typeRef) throws IOException {
        FSDataInputStream in = new FSDataInputStream(new FileInputStream(file));
        initialize(in, file.length(), typeRef);
    }
    
    private void initialize(FSDataInputStream inputStream, long size, TypeRef<M> typeRef) throws IOException {
        this.inputStream = inputStream;
        this.totalSize = size;
        this.converter = ProtobufConverter.newInstance(typeRef);
        this.key = new LongWritable();
        this.value = new ProtobufWritable<M>(typeRef);
        readTrailer(size);
        readIndexEntries();
        moveToNextDataBlock();
    }
    
    private Boolean moveToNextDataBlock() throws IOException {
        if(currentIndex >= indexEntries.size()) {
            messageCount = 0;
            return false;
        }
        currentIndex += 1;
        Tapfile.IndexEntry e = indexEntries.get(currentIndex);
        messageCount = e.getMessageCount();
        inputStream.seek(e.getDataOffset());
        
        byte[] bytes = new byte[8];
        inputStream.read(bytes);
        assertEquals("datagzip", bytes);
        
        dataStream = CodedInputStream.newInstance(new GZIPInputStreamNoClose(inputStream));
        
        return true;
    }
    
    private void readTrailer(long fileSize) throws IOException {
        byte[] sig = new byte[16];
        inputStream.read(fileSize - 16, sig, 0, 16);
        CodedInputStream stream = CodedInputStream.newInstance(sig);
        long trailerOffset = stream.readRawLittleEndian64();
        byte[] bytes = stream.readRawBytes(8);
        assertEquals("tapproto", bytes);
        
        inputStream.seek(trailerOffset);
        stream = CodedInputStream.newInstance(inputStream);
        bytes = stream.readRawBytes(8);
        assertEquals("trainone", bytes);
        
        int limit = stream.pushLimit(stream.readRawVarint32());
        trailer = Tapfile.Header.parseFrom(stream);
        stream.popLimit(limit);
    }
    
    private void readIndexEntries() throws IOException {
       inputStream.seek(trailer.getIndexOffset());
       CodedInputStream stream = CodedInputStream.newInstance(inputStream);
       byte[] bytes = stream.readRawBytes(8);
       assertEquals("upixnone", bytes);
       for(long i = 0; i < trailer.getDataBlockCount(); ++i) {
           int limit = stream.pushLimit(stream.readRawVarint32());
           Tapfile.IndexEntry entry = Tapfile.IndexEntry.parseFrom(stream);
           stream.popLimit(limit);
           indexEntries.add(entry);
       }
    }
    
    
    @Override
    public void close() throws IOException {
        if(inputStream != null)
            inputStream.close();
    }

    @Override
    public LongWritable createKey() {
        return key;
    }

    @Override
    public BinaryWritable<M> createValue() {
        return value;
    }

    @Override
    public long getPos() throws IOException {
        return inputStream.getPos();
    }

    @Override
    public float getProgress() throws IOException {
        return Math.min(1.0f, getPos() / (float) totalSize);
    }

    @Override
    public boolean next(LongWritable k, BinaryWritable<M> v) throws IOException {
        if(messageCount <= 0 && !moveToNextDataBlock())
            return false;
        
        int keySize = dataStream.readRawVarint32();
        byte[] keyBytes = dataStream.readRawBytes(keySize);
       
        key.set(getPos());
        int messageSize = dataStream.readRawVarint32();
        byte[] messageBytes = dataStream.readRawBytes(messageSize); 
        value.set(converter.fromBytes(messageBytes));

        return true;
    }
    
    private static void assertEquals(String expected, byte[] bytes) throws IOException {
        if(!expected.equals(new String(bytes, "ASCII")))
            throw new IOException("failed to read expected tag " + expected);
    }
    
    private static class GZIPInputStreamNoClose extends GZIPInputStream {
        GZIPInputStreamNoClose(InputStream stream) throws IOException {
            super(stream);
        }

        @Override
        public void close() {
            // don't close underlying stream
        }
    }
}
