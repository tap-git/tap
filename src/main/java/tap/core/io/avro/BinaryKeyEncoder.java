package tap.core.io.avro;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

import tap.core.io.SortOrder;

public class BinaryKeyEncoder extends Encoder {
	
	private SortOrder order = SortOrder.ASCENDING;
	private OutputStream out;
	
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
	public void writeNull() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeBoolean(boolean b) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeInt(int n) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeLong(long n) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeFloat(float f) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeDouble(double d) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeString(Utf8 utf8) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeBytes(ByteBuffer bytes) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeBytes(byte[] bytes, int start, int len) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeFixed(byte[] bytes, int start, int len) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeEnum(int e) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeArrayStart() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setItemCount(long itemCount) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startItem() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeArrayEnd() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeMapStart() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeMapEnd() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeIndex(int unionIndex) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
