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
	
	public BinaryKey() {}
	
	public void set(byte[] buf, int length) {
		this.buf = buf;
		this.length = length;
	}
	
	public byte[] getBuffer() {
		return buf;
	}
	
	public int getLength() {
		return length;
	}

	public void set(GenericData.Record record) throws IOException {
		initializeStream();
		
		BinaryKeyDatumWriter.serialize(record, stream);
		
		this.buf = stream.getBuffer();
		this.length = stream.getCount();
	}

	private void initializeStream() {
		if(stream == null)
			stream = new ReuseByteArrayOutputStream(128);
		else
			stream.reset();
	}
}
