package tap.core.io.avro;

import java.io.IOException;

import tap.core.io.SortOrder; 

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

public class BinaryKeyDatumWriter<T> extends GenericDatumWriter<T>{
	private final GenericData data;
	
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
		for (Field f : schema.getFields()) {
			Object value = data.getField(datum, f.name(), f.pos());
			try {
				// we want the encoder to write field and also it's order 
				// the idea is that the binary representation of the record can simply be byte compared
				// without even splitting the fields
				if(out instanceof BinaryKeyEncoder) {
					((BinaryKeyEncoder) out).setSortOrder(
							f.order() == Order.DESCENDING ? SortOrder.DESCENDING : SortOrder.ASCENDING);
				}
				write(f.schema(), value, out);
			} catch (NullPointerException e) {
				throw npe(e, " in field "+f.name());
			}
		}
	}
	
}
