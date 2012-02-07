package tap.core.io;

import java.io.ByteArrayOutputStream;

public class ReuseByteArrayOutputStream extends ByteArrayOutputStream {
	
	public ReuseByteArrayOutputStream() {
		super();
	}
	
	public ReuseByteArrayOutputStream(int size) {
		super(size);
	}
	
	public byte[] getBuffer() {
		return buf;
	}
	public int getCount() {
		return count;
	}
}
