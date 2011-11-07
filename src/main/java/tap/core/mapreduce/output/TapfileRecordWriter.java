package tap.core.mapreduce.output;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.mortbay.servlet.GzipFilter.GzipStream;

import tap.core.mapreduce.io.BinaryWritable;
import tap.formats.tapproto.Tapfile;
import tap.formats.tapproto.Tapfile.IndexEntry;
import tap.util.Protobufs;
import tap.util.TypeRef;

import com.google.common.io.CountingOutputStream;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FileDescriptor;

public class TapfileRecordWriter<M extends Message> implements RecordWriter<NullWritable, BinaryWritable<M>> {
    
    static final int DEFAULT_TARGET_BLOCK_SIZE  = 256 * 1024;
    static final String UTF8 = "UTF-8";
    static final int ALIGNMENT = 512;
    static final byte[] PAD_BYTES = new byte[ALIGNMENT];
    static final ByteString EMPTY_KEY = ByteString.copyFromUtf8("<NEEDS-CODING>");
    
    static {
        Arrays.fill(PAD_BYTES, (byte) 0xFA);
    }

    private FSDataOutputStream fsOutputStream;
    private TypeRef<M> typeRef;
    private Boolean firstWrite = true;
    private CodedOutputStream outputStream;
    private Tapfile.Header.Builder trailerBuilder;
    private ByteArrayOutputStream indexBuffer;
    private CodedOutputStream indexStream;
    private Tapfile.IndexEntry.Builder indexEntryBuilder;
    
    private CountingOutputStream dataCountStream;
    private GZIPOutputStream dataGzipStream;
    private CodedOutputStream dataStream;
    
    public TapfileRecordWriter(Configuration job, Path path, TypeRef<M> typeRef) throws IOException {
        FileSystem fs = path.getFileSystem(job);
        initialize(fs.create(path), typeRef);
    }
    
    public TapfileRecordWriter(Configuration job, Path path, Progressable progress, TypeRef<M> typeRef) throws IOException {
        FileSystem fs = path.getFileSystem(job);
        initialize(fs.create(path, progress), typeRef);
    }
    
    private void initialize(FSDataOutputStream outputStream, TypeRef<M> typeRef) {
        this.fsOutputStream = outputStream;
        this.typeRef = typeRef;
        this.outputStream = CodedOutputStream.newInstance(fsOutputStream);
    }

    @Override
    public void close(Reporter arg0) throws IOException {
        
        flush();
        
        if(firstWrite) { // empty file!
            fsOutputStream.close();
            return;
        }
        
        indexStream.flush();
        indexBuffer.close();
        trailerBuilder.setIndexOffset(fsOutputStream.getPos());
        fsOutputStream.write(indexBuffer.toByteArray());
        fsOutputStream.flush();
        pad(fsOutputStream, 0);
        trailerBuilder.setIndexBytes(fsOutputStream.getPos() - trailerBuilder.getIndexOffset());
       
        Tapfile.Header trailer = trailerBuilder.build();
        long trailerOffset = fsOutputStream.getPos();
        
        dataStream = CodedOutputStream.newInstance(fsOutputStream);
        writeRawBytes(dataStream, "trai");
        writeRawBytes(dataStream, "none");
        dataStream.writeRawVarint32(trailer.getSerializedSize());
        trailer.writeTo(dataStream);
        dataStream.flush();
        pad(fsOutputStream, 16);
        
        dataStream.writeRawLittleEndian64(trailerOffset);
        writeRawBytes(dataStream, "tapproto");
        dataStream.flush();
        
        fsOutputStream.close();
    }

    @Override
    public void write(NullWritable arg0, BinaryWritable<M> writable)
            throws IOException {
        
        if(firstWrite) {
            firstWrite = false;
            Tapfile.Header header = writeHeader();
            initializeTrailer(header);
            initializeIndexStream();
        }
        
        if(dataStream == null) {
           indexEntryBuilder = Tapfile.IndexEntry.newBuilder(); 
           indexEntryBuilder.setFirstKey(EMPTY_KEY);
           indexEntryBuilder.setDataOffset(fsOutputStream.getPos());
           indexEntryBuilder.setMessageCount(0);
           writeRawBytes(fsOutputStream, "data");
           writeRawBytes(fsOutputStream, "gzip");
           
           dataGzipStream = new GZIPOutputStreamNoClose(fsOutputStream);
           dataCountStream = new CountingOutputStream(dataGzipStream);
           dataStream = CodedOutputStream.newInstance(dataCountStream);
        }
        
        dataStream.writeRawVarint32(EMPTY_KEY.size());
        dataStream.writeRawBytes(EMPTY_KEY.toByteArray());
        M msg = writable.get();
        dataStream.writeRawVarint32(msg.getSerializedSize());
        msg.writeTo(dataStream);
        
        indexEntryBuilder.setMessageCount(indexEntryBuilder.getMessageCount() + 1);
        trailerBuilder.setLastKey(EMPTY_KEY);
        dataStream.flush();
        
        if(dataCountStream.getCount() >= DEFAULT_TARGET_BLOCK_SIZE) {
            flush();
        }
    }
    
    private void flush() throws IOException {
        if(dataStream == null)
            return;
       
        long dataByteCount = dataCountStream.getCount();
        trailerBuilder.setDataBlockCount(trailerBuilder.getDataBlockCount() + 1);
        trailerBuilder.setMessageCount(trailerBuilder.getMessageCount() + indexEntryBuilder.getMessageCount());
        trailerBuilder.setUncompressedBytes(trailerBuilder.getUncompressedBytes() + dataByteCount);
        if(trailerBuilder.getMaxDecompSize() < dataByteCount)
            trailerBuilder.setMaxDecompSize(dataByteCount);
  
        dataStream.flush();
        dataGzipStream.finish();
        pad(fsOutputStream, 0);
        indexEntryBuilder.setDataBytes(fsOutputStream.getPos() - indexEntryBuilder.getDataOffset());
        IndexEntry entry = indexEntryBuilder.build();
        indexStream.writeRawVarint32(entry.getSerializedSize());
        entry.writeTo(indexStream);

        dataStream = null;
    }
    
    private Tapfile.Header writeHeader() throws IOException {
        Tapfile.Header.Builder headerBuilder = Tapfile.Header.newBuilder(); 
        headerBuilder.setInitialPipeName("mapred.output");
        headerBuilder.setKeyDescriptor("<NEEDS CODING>");
        
        headerBuilder.setMessageName(typeRef.getRawClass().getName());
        headerBuilder.setTargetDecompSize(DEFAULT_TARGET_BLOCK_SIZE);
        
        writeRawBytes(outputStream, "tapproto");
        writeRawBytes(outputStream, "head");
        writeRawBytes(outputStream, "none");
        
        Tapfile.Header header = headerBuilder.build();
        outputStream.writeRawVarint32(header.getSerializedSize());
        header.writeTo(outputStream);
        outputStream.flush();
        pad(fsOutputStream, 0);
        
        return header;
    }
    
    private void initializeTrailer(Tapfile.Header header) {
        trailerBuilder = Tapfile.Header.newBuilder().mergeFrom(header);
        
        // add descriptors from imported .proto Files to the trailer
        for(FileDescriptor fd : Protobufs.getMessageDescriptor(typeRef.getRawClass()).getFile().getDependencies()) {
            trailerBuilder.addFormatDescriptor(fd.toProto().toByteString());
        }
        
        // add the main File descriptor proto to the trailer
        FileDescriptor fdmain = Protobufs.getMessageDescriptor(typeRef.getRawClass()).getFile();
        trailerBuilder.addFormatDescriptor(fdmain.toProto().toByteString());
    }
    
    private void initializeIndexStream() throws IOException {
        indexBuffer = new ByteArrayOutputStream(1024);
        indexStream = CodedOutputStream.newInstance(indexBuffer);
        writeRawBytes(indexStream, "upix");
        writeRawBytes(indexStream, "none");
    }

    static void writeRawBytes(CodedOutputStream stream, String data) throws IOException {
        stream.writeRawBytes(data.getBytes(UTF8));
    }
    
    static void writeRawBytes(OutputStream stream, String data) throws IOException {
        stream.write(data.getBytes(UTF8));
    }
    
    static void pad(FSDataOutputStream out, int remnant) throws IOException {
        long padBytes = ALIGNMENT - 1 - ((out.getPos() + ALIGNMENT-1) % ALIGNMENT);
        padBytes -= remnant;
        out.write(PAD_BYTES, 0, (int) padBytes);
    }
    
    private static class GZIPOutputStreamNoClose extends GZIPOutputStream {
        GZIPOutputStreamNoClose(OutputStream stream) throws IOException {
            super(stream);
        }

        @Override
        public void close() {
            // don't close underlying stream
        }
    }
}
