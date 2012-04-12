package tap.core.mapreduce.input;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import tap.core.io.BinaryKey;
import tap.core.mapreduce.io.BinaryWritable;
import tap.core.mapreduce.io.ProtobufConverter;
import tap.core.mapreduce.io.ProtobufWritable;
import tap.formats.Formats;
import tap.formats.tapproto.Tapfile;
import tap.formats.tapproto.Testmsg.TestRecord;
import tap.util.Protobufs;
import tap.util.TypeRef;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;

public class TapfileRecordReader<M extends Message> implements RecordReader<BinaryKey, BinaryWritable<M>> {

    private FSDataInputStream inputStream;
    private long totalSize;
    private Tapfile.Header trailer;
    private List<Tapfile.IndexEntry> indexEntries = new ArrayList<Tapfile.IndexEntry>();
    private ProtobufConverter<M> converter;
    private BinaryKey key;
    private ProtobufWritable<M> value;
    private int currentIndex = -1;
    private long messageCount;
    
    private CodedInputStream dataStream;
    
    public TapfileRecordReader(Configuration job, FileSplit split, TypeRef<M> typeRef) throws IOException {
        this(job, split.getPath(), typeRef);
    }
    
    public TapfileRecordReader(Configuration job, Path file, TypeRef<M> typeRef) throws IOException {
        FileSystem fs = file.getFileSystem(job);
        FileStatus status = fs.getFileStatus(file);
        //late binding file format check
        
        Formats fileFormat = sniffFileFormat(fs, file);
        if(fileFormat != Formats.TAPPROTO_FORMAT)
        	throw new IOException("Tried to read a " + fileFormat.toString() + " .  Expecting tapproto.");
        
        
        initialize(fs.open(file), status.getLen(), typeRef);
    }
    
    //for tap.subscribe, need to determine the TypeRef from the file
    public TapfileRecordReader(Configuration job, Path file) throws IOException {
    	FileSystem fs = file.getFileSystem(job);
        FileStatus status = fs.getFileStatus(file);
        this.inputStream = fs.open(file);
        this.totalSize = status.getLen();
        readTrailer(totalSize);
        Class<? extends Message> message = Protobufs.getProtobufClass(trailer.getMessageName());
        //message is a class e.g., 
        TypeRef typeRef = new TypeRef(message){}; 
        this.converter = ProtobufConverter.newInstance(typeRef);
        this.key = new BinaryKey();
        this.value = new ProtobufWritable<M>(typeRef);
        readIndexEntries();
        moveToNextDataBlock();
        
    }
    //just a test
    
    public static Class readMessageClass(Configuration job, Path file) throws IOException
    {
    	FileSystem fs = file.getFileSystem(job);
        FileStatus status = fs.getFileStatus(file);
        FSDataInputStream inputStream = fs.open(file);
        long totalSize = status.getLen();
        byte[] sig = new byte[16];
        
        inputStream.read(totalSize - 16, sig, 0, 16);
        CodedInputStream stream = CodedInputStream.newInstance(sig);
        long trailerOffset = stream.readRawLittleEndian64();
        byte[] bytes = stream.readRawBytes(8);
        assertEquals("tapproto", bytes);
        
        inputStream.seek(trailerOffset);
        stream = CodedInputStream.newInstance(inputStream);
        bytes = stream.readRawBytes(8);
        assertEquals("trainone", bytes);
        
        int limit = stream.pushLimit(stream.readRawVarint32());
        Tapfile.Header trailer = Tapfile.Header.parseFrom(stream);
        List<ByteString> f =  trailer.getFormatDescriptorList();
        stream.popLimit(limit);
        Class<? extends Message> message = Protobufs.getProtobufClass(trailer.getMessageName());
        return message;
    }
    
    
    
    private void initialize(FSDataInputStream inputStream, long size, TypeRef<M> typeRef) throws IOException {
    	
        this.inputStream = inputStream;
        this.totalSize = size;
        this.converter = ProtobufConverter.newInstance(typeRef);
        this.key = new BinaryKey();
        this.value = new ProtobufWritable<M>(typeRef);
        readTrailer(size);
        readIndexEntries();
        moveToNextDataBlock();
    }
    
    
    private Formats sniffFileFormat(FileSystem fs, Path path) throws IOException, FileNotFoundException {

    	byte[] header;
    	FSDataInputStream in = null;
    	try {
    		in = fs.open(path);
    		header = new byte[1000];
    		in.read(header);
    		in.close();
    		
    	} finally {
    		if(in != null)
    			in.close();
    	}
    	return determineFileFormat(header);
    	
}


private Formats determineFileFormat(byte[] header) {
    for (Formats format : Formats.values()) {
        if (format.getFileFormat().signature(header)) {
            return format;

        }
    }
    return Formats.UNKNOWN_FORMAT;
}

    private Boolean moveToNextDataBlock() throws IOException {
        if(++currentIndex >= indexEntries.size()) {
            messageCount = 0;
            return false;
        }
        Tapfile.IndexEntry e = indexEntries.get(currentIndex);
        messageCount = e.getMessageCount();
        inputStream.seek(e.getDataOffset());
        
        byte[] bytes = new byte[8];
        inputStream.read(bytes);
        //assertEquals("datagzip", bytes);
        
        inputStream.seek(e.getDataOffset()+8);
        
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
    public BinaryKey createKey() {
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
    public boolean next(BinaryKey k, BinaryWritable<M> v) throws IOException {
        if(messageCount <= 0 && !moveToNextDataBlock())
            return false;
        
        int keySize = dataStream.readRawVarint32();
        byte[] keyBytes = dataStream.readRawBytes(keySize);
        
        key.set(keyBytes, keySize);
        
        int messageSize = dataStream.readRawVarint32();
        byte[] messageBytes = dataStream.readRawBytes(messageSize); 
        value.set(converter.fromBytes(messageBytes));
        
        messageCount -= 1;

        return true;
    }
    
    //to implement subscribe
    public boolean hasNext() 
    {
    	try {
			if(messageCount <=0 && !moveToNextDataBlock())
				return false;
			else
				return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
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
