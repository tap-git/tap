package tap.core.io.avro;

import java.io.IOException;

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
	
	private ReuseByteArrayOutputStream buf = new ReuseByteArrayOutputStream(256);
	private BinaryKeyEncoder encoder = new BinaryKeyEncoder(buf);
	
	public BinaryKeyDatumWriter(Schema root) {
		this(root, GenericData.get());
	}
	
	public BinaryKeyDatumWriter(Schema root, GenericData data) {
		super(root, data);
		this.data = data;
	}
	
	@Override
	protected void writeRecord(Schema schema, Object datum, Encoder out)
			throws IOException {
		
		buf.reset();
		
		// write fields to buffer
		for (Field f : schema.getFields()) {
			Object value = data.getField(datum, f.name(), f.pos());
			try {
				encoder.setSortOrder(
					f.order() == Order.DESCENDING ? SortOrder.DESCENDING : SortOrder.ASCENDING);
				write(f.schema(), value, encoder);
			} catch (NullPointerException e) {
				throw npe(e, " in field "+f.name());
			}
		}
		
		// write buffer to out
		out.writeBytes(buf.getBuffer(), 0, buf.getCount());
	}
	
}
