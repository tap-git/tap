package tap.core.io.avro;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class BinaryKeyDecoder extends Decoder {

	@Override
	public void readNull() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean readBoolean() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int readInt() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long readLong() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float readFloat() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double readDouble() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Utf8 readString(Utf8 old) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void skipString() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ByteBuffer readBytes(ByteBuffer old) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void skipBytes() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readFixed(byte[] bytes, int start, int length)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void skipFixed(int length) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int readEnum() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long readArrayStart() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long arrayNext() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long skipArray() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long readMapStart() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long mapNext() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long skipMap() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int readIndex() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
