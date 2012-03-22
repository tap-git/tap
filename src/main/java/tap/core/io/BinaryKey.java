package tap.core.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import tap.core.io.avro.BinaryKeyDatumWriter;

public class BinaryKey {
	private byte[] buf;
	private int length;
	private int groupLength;
	private ReuseByteArrayOutputStream stream;
	private GenericData.Record record;
	private boolean dirty;
	
	public static final int KEY_BYTES_OFFSET = Bytes.SIZEOF_INT * 2; // length, group length
	
	public BinaryKey() {}
	
	public void set(byte[] buf, int length) {
		this.buf = buf;
		this.length = length;
		this.groupLength = Bytes.toInt(this.buf, Bytes.SIZEOF_INT, SortOrder.ASCENDING); 
		this.dirty = false;
	}
	
	public byte[] getBuffer() {
		serializeIfNecessary();
		return buf;
	}
	
	public int getLength() {
		serializeIfNecessary();
		return length;
	}
	
	public int keyBytesLength() {
		serializeIfNecessary();
		return getLength() - KEY_BYTES_OFFSET;
	}
	
	public int groupBytesLength() {
		serializeIfNecessary();
		return groupLength;
	}
	
	public void setSchema(Schema schema) {
		record = new GenericData.Record(schema);
	}
	
	public void setField(String name, Object value) {
		record.put(name, value);
		dirty = true;
	}
	
	public void dirty()
	{
		dirty=true;
	}
	
	
	private void serializeIfNecessary() {
		if(dirty == false)
			return;
		
		if(stream == null)
			stream = new ReuseByteArrayOutputStream(128);
		else
			stream.reset();
		
		try {
			BinaryKeyDatumWriter.serialize(record, stream);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		
		this.buf = stream.getBuffer();
		this.length = stream.getCount();
		this.groupLength = Bytes.toInt(this.buf, Bytes.SIZEOF_INT, SortOrder.ASCENDING); 
		dirty = false;
	}
}
