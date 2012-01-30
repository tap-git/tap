package tap.core.io.avro;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

public class BinaryKeyDecoder extends Decoder {
	
	private InputStream in;
	
	private Decoder decoder;
	
	public BinaryKeyDecoder(InputStream in) {
		this.in = in;
		decoder = DecoderFactory.get().directBinaryDecoder(in, null);
	}

	@Override
	public void readNull() throws IOException {
		decoder.readNull();
	}

	@Override
	public boolean readBoolean() throws IOException {
		return decoder.readBoolean();
	}

	@Override
	public int readInt() throws IOException {
		return decoder.readInt();
	}

	@Override
	public long readLong() throws IOException {
		return decoder.readLong();
	}

	@Override
	public float readFloat() throws IOException {
		return decoder.readFloat();
	}

	@Override
	public double readDouble() throws IOException {
		return decoder.readDouble();
	}

	@Override
	public Utf8 readString(Utf8 old) throws IOException {
		return decoder.readString(old);
	}

	@Override
	public void skipString() throws IOException {
		decoder.skipString();
	}

	@Override
	public ByteBuffer readBytes(ByteBuffer old) throws IOException {
		return decoder.readBytes(old);
	}

	@Override
	public void skipBytes() throws IOException {
		decoder.skipBytes();
	}

	@Override
	public void readFixed(byte[] bytes, int start, int length)
			throws IOException {
		decoder.readFixed(bytes, start, length);
	}

	@Override
	public void skipFixed(int length) throws IOException {
		decoder.skipFixed(length);
	}

	@Override
	public int readEnum() throws IOException {
		return decoder.readEnum();
	}

	@Override
	public long readArrayStart() throws IOException {
		return decoder.readArrayStart();
	}

	@Override
	public long arrayNext() throws IOException {
		return decoder.arrayNext();
	}

	@Override
	public long skipArray() throws IOException {
		return decoder.skipArray();
	}

	@Override
	public long readMapStart() throws IOException {
		return decoder.readMapStart();
	}

	@Override
	public long mapNext() throws IOException {
		return decoder.mapNext();
	}

	@Override
	public long skipMap() throws IOException {
		return decoder.skipMap();
	}

	@Override
	public int readIndex() throws IOException {
		return decoder.readIndex();
	}

}
