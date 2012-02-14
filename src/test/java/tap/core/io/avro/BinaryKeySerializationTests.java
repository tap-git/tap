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

import tap.core.BinaryKeyComparator;
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
		
		// set age as a sort field
		schema_asc.getField("age").addProp("x-sort", "true");
		schema_desc.getField("age").addProp("x-sort", "true");
	}
	
	private BinaryKey encode(String name, int age, Schema schema) throws IOException {
		BinaryKey key = new BinaryKey();
		
		key.setSchema(schema);
		key.setField("name", name);
		key.setField("age", age);
		
		return key;
	}
	
	private BinaryKey decode(byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		BinaryKeyDatumReader reader = new BinaryKeyDatumReader();
		Decoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
		return reader.read(null, decoder);
	}
	
	@Test
	public void writeRecord() throws IOException {
		BinaryKey key = encode("john", 30, schema_asc);
		
		Assert.assertEquals(19, key.getLength());
		
		byte[] bytes = key.getBuffer();
		
		// first 4 bytes is length of key written asc
		int dataSize = Bytes.toInt(bytes, 0, SortOrder.ASCENDING);
		Assert.assertEquals(11, dataSize);
		
		// next 4 bytes is group length (name)
		int groupLength = Bytes.toInt(bytes, 4, SortOrder.ASCENDING);
		Assert.assertEquals(6, groupLength);

		// string tag
		Assert.assertEquals(Types.STRING.asc(), bytes[8] & 0xff);
		
		// name
		byte[] name = { 'j', 'o', 'h', 'n', Bytes.TERM };
		Assert.assertTrue(Bytes.compare(name, 0, 5, bytes, 9, 5) == 0);
		
		// int tag 
		Assert.assertEquals(Types.INT.asc(), bytes[14] & 0xff);
		
		// age
		int age = Bytes.toInt(bytes, 15, SortOrder.ASCENDING);
		Assert.assertEquals(30, age);
	}
	
	@Test
	public void writeRecordDesc() throws IOException {
		BinaryKey key = encode("john", 30, schema_desc);
		
		Assert.assertEquals(19, key.getLength());
		
		byte[] bytes = key.getBuffer();
		
		// first 4 bytes is length of key written asc
		int dataSize = Bytes.toInt(bytes, 0, SortOrder.ASCENDING);
		Assert.assertEquals(11, dataSize);
		
		// next 4 bytes is group length (name)
		int groupLength = Bytes.toInt(bytes, 4, SortOrder.ASCENDING);
		Assert.assertEquals(6, groupLength);

		// string tag
		Assert.assertEquals(Types.STRING.desc(), bytes[8] & 0xff);
		
		// name desc
		byte[] name = {
			(byte) Bytes.UBYTE_MAX_VALUE - 'j',
			(byte) Bytes.UBYTE_MAX_VALUE - 'o',
			(byte) Bytes.UBYTE_MAX_VALUE - 'h',
			(byte) Bytes.UBYTE_MAX_VALUE - 'n',
			(byte) Bytes.UBYTE_MAX_VALUE - Bytes.TERM };
		
		Assert.assertTrue(Bytes.compare(name, 0, 5, bytes, 9, 5) == 0);
		
		// int tag 
		Assert.assertEquals(Types.INT.desc(), bytes[14] & 0xff);
		
		// age
		int age = Bytes.toInt(bytes, 15, SortOrder.DESCENDING);
		Assert.assertEquals(30, age);
	}
	
	@Test
	public void compareAsc() throws IOException {
		BinaryKeyComparator comparator = new BinaryKeyComparator();
		
		BinaryKey k1 = encode("john", 30, schema_asc);
		BinaryKey k2 = encode("john", 31, schema_asc);
		
		byte[] b1 = k1.getBuffer();
		byte[] b2 = k2.getBuffer();
		int b1_length = k1.getLength();
		int b2_length = k2.getLength();
		
		// b1 == b1
		int result = comparator.compare(b1, 0, b1_length, b1, 0, b1_length);
		Assert.assertEquals(0, result);
		
		// b1 < b2
		result = comparator.compare(b1, 0, b1_length, b2, 0, b2_length);
		Assert.assertTrue(result < 0);
		
		// b2 > b1
		result = comparator.compare(b2, 0, b2_length, b1, 0, b1_length);
		Assert.assertTrue(result > 0);
	}
	
	@Test
	public void compareDesc() throws IOException {
		BinaryKeyComparator comparator = new BinaryKeyComparator();
		
		BinaryKey k1 = encode("john", 30, schema_desc);
		BinaryKey k2 = encode("john", 31, schema_desc);
		
		byte[] b1 = k1.getBuffer();
		byte[] b2 = k2.getBuffer();
		int b1_length = k1.getLength();
		int b2_length = k2.getLength();
		
		// b1 == b1
		int result = comparator.compare(b1, 0, b1_length, b1, 0, b1_length);
		Assert.assertEquals(0, result);
		
		// b1 > b2
		result = comparator.compare(b1, 0, b1_length, b2, 0, b2_length);
		Assert.assertTrue(result > 0);
		
		// b2 < b1
		result = comparator.compare(b2, 0, b2_length, b1, 0, b1_length);
		Assert.assertTrue(result < 0);
	}
	
	@Test
	public void readBinaryKey() throws IOException {
		BinaryKey original = encode("john", 30, schema_asc);
		BinaryKey key = decode(original.getBuffer());
		
		Assert.assertEquals(19, key.getLength());
		
		Assert.assertNotSame(key.getBuffer(), original.getBuffer());
		Assert.assertEquals(0, Bytes.compare(
				key.getBuffer(), 0, key.getLength(),
				original.getBuffer(), 0, original.getLength()));
		
		Assert.assertEquals(11, key.keyBytesLength());
		Assert.assertEquals(6, key.groupBytesLength());
	}
}
