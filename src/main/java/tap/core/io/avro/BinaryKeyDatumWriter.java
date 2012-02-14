package tap.core.io.avro;

import java.io.IOException;
import java.io.OutputStream;

import tap.core.io.BinaryKey;
import tap.core.io.Bytes;
import tap.core.io.ReuseByteArrayOutputStream;
import tap.core.io.SortOrder; 

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

public class BinaryKeyDatumWriter<T> extends GenericDatumWriter<T>{
	private final GenericData data;
	private byte[] intbuf = new byte[Bytes.SIZEOF_INT];
	
	private static class EncodingStream extends BinaryKeyEncoder {
		private ReuseByteArrayOutputStream out;
		private EncodingStream() {
			this(new ReuseByteArrayOutputStream(256));
		}
		private EncodingStream(ReuseByteArrayOutputStream out) {
			super(out);
			this.out = out;
		}
		private void reset() {
			out.reset();
		}
		private byte[] buffer() {
			return out.getBuffer();
		}
		private int count() {
			return out.getCount();
		}
	}
	
	private static final ThreadLocal<EncodingStream> threadLocalStream = new ThreadLocal<EncodingStream>() {
		@Override
		protected EncodingStream initialValue() {
			return new EncodingStream();
		}
	};
	
	@SuppressWarnings("rawtypes")
	private static final ThreadLocal<BinaryKeyDatumWriter> instance = new ThreadLocal<BinaryKeyDatumWriter>() {
		@Override
		protected BinaryKeyDatumWriter initialValue() {
			return new BinaryKeyDatumWriter();
		}
	};

	private BinaryKeyDatumWriter() {
		this(GenericData.get());
	}
	
	private BinaryKeyDatumWriter(GenericData data) {
		super();
		this.data = data;
	}
	
	public BinaryKeyDatumWriter(Schema root) {
		this(root, GenericData.get());
 	}
 	
	public BinaryKeyDatumWriter(Schema root, GenericData data) {
		super(root, data);
 		this.data = data;
 	}
	
	public static void serialize(GenericData.Record record, OutputStream out) throws IOException {
		instance.get().doSerialize(record, out);
	}
	
	private void doSerialize(GenericData.Record record, OutputStream out) throws IOException {
		this.setSchema(record.getSchema());
		
		EncodingStream stream = threadLocalStream.get();
		stream.reset();
		int groupLength = doWriteRecord(stream, record.getSchema(), record);

		// write key length to out
		Bytes.putInt(intbuf, 0, stream.count(), SortOrder.ASCENDING);
		out.write(intbuf);
		
		// write group length
		Bytes.putInt(intbuf, 0, groupLength, SortOrder.ASCENDING);
		out.write(intbuf);
		
		// write buffer to out
		out.write(stream.buffer(), 0, stream.count());
	}
	
	@Override
	protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
		EncodingStream stream = threadLocalStream.get();
		stream.reset();
		int groupLength = doWriteRecord(stream, schema, datum);
		
		// write key length to out
		Bytes.putInt(intbuf, 0, stream.count(), SortOrder.ASCENDING);
		out.writeFixed(intbuf);
		
		// write group length
		Bytes.putInt(intbuf, 0, groupLength, SortOrder.ASCENDING);
		out.writeFixed(intbuf);
		
		// write buffer to out
		out.writeFixed(stream.buffer(), 0, stream.count());
	}
	
	private int doWriteRecord(EncodingStream stream, Schema schema, Object datum) throws IOException {
		int groupLength = 0;
		
		for (Field f : schema.getFields()) {
			Object value = data.getField(datum, f.name(), f.pos());
			try {
				stream.setSortOrder(
					f.order() == Order.DESCENDING ? SortOrder.DESCENDING : SortOrder.ASCENDING);
				write(f.schema(), value, stream);
				boolean isSort = "true".equals(f.getProp("x-sort")); // set in Phase.groupAndSort
				if(isSort == false) {
					groupLength = stream.count();
				}
			} catch (NullPointerException e) {
				throw npe(e, " in field "+f.name());
			}
		}		
		return groupLength;
	}
	
}
