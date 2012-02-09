package tap.core.io.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import tap.core.KeyComparator;
import tap.core.io.BinaryKey;
import tap.core.io.Bytes;
import tap.core.io.SortOrder;
import tap.core.io.Types;

public class BinaryKeySerializationTests {

	private static final Schema STRING = Schema.create(Schema.Type.STRING);
	private static final Schema INT = Schema.create(Schema.Type.INT);
	private Schema schema_asc, schema_desc;
	
	@Before
	public void setup() {
		
		List<Field> asc = new ArrayList<Field>();
		asc.add(new Field("name", STRING, null, null, Order.ASCENDING));
		asc.add(new Field("age", INT, null, null, Order.ASCENDING));
		
		List<Field> desc = new ArrayList<Field>();
		desc.add(new Field("name", STRING, null, null, Order.DESCENDING));
		desc.add(new Field("age", INT, null, null, Order.DESCENDING));
		
		schema_asc = Schema.createRecord(asc);
		schema_desc = Schema.createRecord(desc);
	}
	
	private byte[] encode(String name, int age, Schema schema) throws IOException {
		GenericData.Record datum = new GenericData.Record(schema);
		datum.put("name", name);
		datum.put("age", age);
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryKeyEncoder encoder = new BinaryKeyEncoder(out);
		BinaryKeyDatumWriter writer = new BinaryKeyDatumWriter(schema);
		writer.write(datum, encoder);
	
		return out.toByteArray();
	}
	
	private BinaryKey decode(byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		BinaryKeyDatumReader reader = new BinaryKeyDatumReader();
		Decoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
		return reader.read(null, decoder);
	}
	
	@Test
	public void writeRecord() throws IOException {
		byte[] bytes = encode("john", 30, schema_asc);
		
		Assert.assertEquals(15, bytes.length);
		
		// first 4 bytes is length of data written asc
		int dataSize = Bytes.toInt(bytes, 0, SortOrder.ASCENDING);
		Assert.assertEquals(11, dataSize);

		// string tag
		Assert.assertEquals(Types.STRING.asc(), bytes[4] & 0xff);
		
		// name
		byte[] name = { 'j', 'o', 'h', 'n', Bytes.TERM };
		Assert.assertTrue(Bytes.compare(name, 0, 5, bytes, 5, 5) == 0);
		
		// int tag 
		Assert.assertEquals(Types.INT.asc(), bytes[10] & 0xff);
		
		// age
		int age = Bytes.toInt(bytes, 11, SortOrder.ASCENDING);
		Assert.assertEquals(30, age);
	}
	
	@Test
	public void writeRecordDesc() throws IOException {
		byte[] bytes = encode("john", 30, schema_desc);
		
		Assert.assertEquals(15, bytes.length);
		
		// first 4 bytes is length of data written asc
		int dataSize = Bytes.toInt(bytes, 0, SortOrder.ASCENDING);
		Assert.assertEquals(11, dataSize);

		// string tag
		Assert.assertEquals(Types.STRING.desc(), bytes[4] & 0xff);
		
		// name desc
		byte[] name = {
			(byte) Bytes.UBYTE_MAX_VALUE - 'j',
			(byte) Bytes.UBYTE_MAX_VALUE - 'o',
			(byte) Bytes.UBYTE_MAX_VALUE - 'h',
			(byte) Bytes.UBYTE_MAX_VALUE - 'n',
			(byte) Bytes.UBYTE_MAX_VALUE - Bytes.TERM };
		
		Assert.assertTrue(Bytes.compare(name, 0, 5, bytes, 5, 5) == 0);
		
		// int tag 
		Assert.assertEquals(Types.INT.desc(), bytes[10] & 0xff);
		
		// age
		int age = Bytes.toInt(bytes, 11, SortOrder.DESCENDING);
		Assert.assertEquals(30, age);
	}
	
	@Test
	public void compareAsc() throws IOException {
		KeyComparator comparator = new KeyComparator();
		
		byte[] b1 = encode("john", 30, schema_asc);
		byte[] b2 = encode("john", 31, schema_asc);
		
		// b1 == b1
		int result = comparator.compare(b1, 0, b1.length, b1, 0, b1.length);
		Assert.assertEquals(0, result);
		
		// b1 < b2
		result = comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
		Assert.assertTrue(result < 0);
		
		// b2 > b1
		result = comparator.compare(b2, 0, b2.length, b1, 0, b1.length);
		Assert.assertTrue(result > 0);
	}
	
	@Test
	public void compareDesc() throws IOException {
		KeyComparator comparator = new KeyComparator();
		
		byte[] b1 = encode("john", 30, schema_desc);
		byte[] b2 = encode("john", 31, schema_desc);
		
		// b1 == b1
		int result = comparator.compare(b1, 0, b1.length, b1, 0, b1.length);
		Assert.assertEquals(0, result);
		
		// b1 > b2
		result = comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
		Assert.assertTrue(result > 0);
		
		// b2 < b1
		result = comparator.compare(b2, 0, b2.length, b1, 0, b1.length);
		Assert.assertTrue(result < 0);
	}
	
	@Test
	public void readBinaryKey() throws IOException {
		byte[] bytes = encode("john", 30, schema_asc);
		BinaryKey key = decode(bytes); 
		Assert.assertEquals(11, key.getLength());
		Assert.assertEquals(11, key.getBuffer().length);
		Assert.assertEquals(0, Bytes.compare(bytes, 4, 11, key.getBuffer(), 0, 11));
	}
}
