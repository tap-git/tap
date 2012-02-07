package tap.core.io;

public class BinaryKey {
	private byte[] buf;
	private int length;

	public BinaryKey() {}
	
	public BinaryKey(byte[] buf, int length) {
		this.buf = buf;
		this.length = length;
	}
	
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
}
