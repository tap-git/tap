package tap.core.io.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;

public class BinaryKeyDatumReader<T> extends GenericDatumReader<T> {
	public BinaryKeyDatumReader(Schema root) {
		super(root);
	}
}
