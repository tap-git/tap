package tap.core.io.avro;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

import tap.core.io.Bytes;
import tap.core.io.SortOrder;
import tap.core.io.Types;

public class BinaryKeyEncoder extends Encoder {
	
	private SortOrder order = SortOrder.ASCENDING;
	private OutputStream out;
	
	private byte[] bytes = new byte[8];
			
	public BinaryKeyEncoder(OutputStream out) {
		this.out = out;
	}
	
	public void setSortOrder(SortOrder order) {
		this.order = order;
	}

	@Override
	public void flush() throws IOException {
		out.flush();
	}

	@Override
	public void writeBoolean(boolean b) throws IOException {
		out.write(Types.BOOL.value(order));
		Bytes.putBoolean(bytes, 0, b, order);
		out.write(bytes, 0, 1);
	}

	@Override
	public void writeInt(int n) throws IOException {
		out.write(Types.INT.value(order));
		Bytes.putInt(bytes, 0, n, order);
		out.write(bytes, 0, Bytes.SIZEOF_INT);
	}

	@Override
	public void writeLong(long n) throws IOException {
		out.write(Types.LONG.value(order));
		Bytes.putLong(bytes, 0, n, order);
		out.write(bytes, 0, Bytes.SIZEOF_LONG);
	}
	
	@Override
	public void writeString(Utf8 utf8) throws IOException {
		out.write(Types.STRING.value(order));
		Bytes.writeString(utf8, out, order);
	}
	
	@Override
	public void writeBytes(byte[] data, int start, int len) throws IOException {
		Bytes.putInt(this.bytes, 0, len, SortOrder.ASCENDING);
		out.write(this.bytes, 0, Bytes.SIZEOF_INT);
		out.write(data, start, len);
	}
	
	@Override
	public void writeNull() throws IOException {
		throw new UnsupportedOperationException("writeNull not yet implemented");
	}

	@Override
	public void writeFloat(float f) throws IOException {
		throw new UnsupportedOperationException("writeFloat not yet implemented");
	}

	@Override
	public void writeDouble(double d) throws IOException {
		throw new UnsupportedOperationException("writeDouble not yet implemented");
	}


	@Override
	public void writeBytes(ByteBuffer bytes) throws IOException {
		throw new UnsupportedOperationException("writeBytes(ByteBuffer) not yet implemented");
	}


	@Override
	public void writeFixed(byte[] bytes, int start, int len) throws IOException {
		throw new UnsupportedOperationException("writeFixed not yet implemented");
	}

	@Override
	public void writeEnum(int e) throws IOException {
		throw new UnsupportedOperationException("writeEnum not yet implemented");
	}

	@Override
	public void writeArrayStart() throws IOException {
		throw new UnsupportedOperationException("writeArrayStart not yet implemented");
	}

	@Override
	public void setItemCount(long itemCount) throws IOException {
		throw new UnsupportedOperationException("setItemCount not yet implemented");
	}

	@Override
	public void startItem() throws IOException {
		throw new UnsupportedOperationException("startItem not yet implemented");
	}

	@Override
	public void writeArrayEnd() throws IOException {
		throw new UnsupportedOperationException("writeArrayEnd not yet implemented");
	}

	@Override
	public void writeMapStart() throws IOException {
		throw new UnsupportedOperationException("writeMapStart not yet implemented");
	}

	@Override
	public void writeMapEnd() throws IOException {
		throw new UnsupportedOperationException("writeMapEnd not yet implemented");
	}

	@Override
	public void writeIndex(int unionIndex) throws IOException {
		throw new UnsupportedOperationException("writeIndex not yet implemented");
	}

}
