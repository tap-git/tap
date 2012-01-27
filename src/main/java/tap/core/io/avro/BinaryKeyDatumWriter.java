package tap.core.io.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;

public class BinaryKeyDatumWriter<T> extends GenericDatumWriter<T>{
	public BinaryKeyDatumWriter(Schema root) {
		super(root);
	}
}
