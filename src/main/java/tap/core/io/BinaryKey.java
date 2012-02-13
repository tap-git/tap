package tap.core.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import tap.core.io.avro.BinaryKeyDatumWriter;

public class BinaryKey {
	private byte[] buf;
	private int length;
	private ReuseByteArrayOutputStream stream;
	private GenericData.Record record;
	private boolean dirty;
	
	public static final int KEY_BYTES_OFFSET = Bytes.SIZEOF_INT;
	
	public BinaryKey() {}
	
	public void set(byte[] buf, int length) {
		this.buf = buf;
		this.length = length;
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
		return getLength() - KEY_BYTES_OFFSET;
	}
	
	public void setSchema(Schema schema) {
		record = new GenericData.Record(schema);
	}
	
	public void setField(String name, Object value) {
		record.put(name, value);
		dirty = true;
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
		
		dirty = false;
	}
}
