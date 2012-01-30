package tap.core.io.avro;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

import tap.core.io.SortOrder;

public class BinaryKeyEncoder extends Encoder {
	
	private SortOrder order = SortOrder.ASCENDING;
	private OutputStream out;
	
	private Encoder encoder;
			
	public BinaryKeyEncoder(OutputStream out) {
		this.out = out;
		encoder = EncoderFactory.get().directBinaryEncoder(out, null);
	}
	
	public void setSortOrder(SortOrder order) {
		this.order = order;
	}

	@Override
	public void flush() throws IOException {
		encoder.flush();
		out.flush();
	}

	@Override
	public void writeNull() throws IOException {
		encoder.writeNull();
	}

	@Override
	public void writeBoolean(boolean b) throws IOException {
		encoder.writeBoolean(b);
	}

	@Override
	public void writeInt(int n) throws IOException {
		encoder.writeInt(n);
	}

	@Override
	public void writeLong(long n) throws IOException {
		encoder.writeLong(n);
	}

	@Override
	public void writeFloat(float f) throws IOException {
		encoder.writeFloat(f);
	}

	@Override
	public void writeDouble(double d) throws IOException {
		encoder.writeDouble(d);
	}

	@Override
	public void writeString(Utf8 utf8) throws IOException {
		encoder.writeString(utf8);
	}

	@Override
	public void writeBytes(ByteBuffer bytes) throws IOException {
		encoder.writeBytes(bytes);
	}

	@Override
	public void writeBytes(byte[] bytes, int start, int len) throws IOException {
		encoder.writeBytes(bytes, start, len);
	}

	@Override
	public void writeFixed(byte[] bytes, int start, int len) throws IOException {
		encoder.writeFixed(bytes, start, len);
	}

	@Override
	public void writeEnum(int e) throws IOException {
		encoder.writeEnum(e);
	}

	@Override
	public void writeArrayStart() throws IOException {
		encoder.writeArrayStart();
	}

	@Override
	public void setItemCount(long itemCount) throws IOException {
		encoder.setItemCount(itemCount);
	}

	@Override
	public void startItem() throws IOException {
		encoder.startItem();
	}

	@Override
	public void writeArrayEnd() throws IOException {
		encoder.writeArrayEnd();
	}

	@Override
	public void writeMapStart() throws IOException {
		encoder.writeMapStart();
	}

	@Override
	public void writeMapEnd() throws IOException {
		encoder.writeMapEnd();
	}

	@Override
	public void writeIndex(int unionIndex) throws IOException {
		encoder.writeIndex(unionIndex);
	}

}
