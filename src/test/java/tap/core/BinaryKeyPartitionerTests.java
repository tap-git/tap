package tap.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.mapred.AvroKey;
import org.junit.Before;
import org.junit.Test;

import tap.core.io.BinaryKey;
import tap.formats.avro.BinaryKeyPartitioner;

import static org.junit.Assert.*;

public class BinaryKeyPartitionerTests {
	private static final Schema STRING = Schema.create(Schema.Type.STRING);
	private static final Schema INT = Schema.create(Schema.Type.INT);
	private Schema schema;
	private MockPartitioner partitioner;
	private AvroKey<BinaryKey> avroKey;
	
	@Before
	public void setup() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("name", STRING, null, null, Order.ASCENDING));
		fields.add(new Field("age", INT, null, null, Order.ASCENDING));
		schema = Schema.createRecord(fields);
		partitioner = new MockPartitioner();
		avroKey = new AvroKey<BinaryKey>();
	}
	
	static class MockPartitioner extends BinaryKeyPartitioner<String> {
		byte[] bytes;
		int byteOffset, byteLength;
		
		@Override
		protected int hashBytes(byte[] bytes, int offset, int length) {
			this.bytes = bytes;
			this.byteOffset = offset;
			this.byteLength = length;
			return super.hashBytes(bytes, offset, length);
		}
	}
	
	private BinaryKey createKey(String name, int age) {
		BinaryKey key = new BinaryKey();
		key.setSchema(schema);
		key.setField("name", name);
		key.setField("age", age);
		return key;
	}
	
	@Test
	public void willUseGroupByBytes() {
		schema.getField("age").addProp("x-sort", "true");
		
		BinaryKey key = createKey("john", 30);
		avroKey.datum(key);
		partitioner.getPartition(avroKey, null, 2);
		
		assertSame(key.getBuffer(), partitioner.bytes);
		assertEquals(BinaryKey.KEY_BYTES_OFFSET, partitioner.byteOffset);
		
		assertFalse(key.groupBytesLength() == key.keyBytesLength());
		assertEquals(key.groupBytesLength(), partitioner.byteLength);
		assertTrue(key.groupBytesLength() > 0);
	}
	
	@Test
	public void willUseKeyBytesIfGroupByBytesZero() {
		// only sort by fields
		schema.getField("name").addProp("x-sort", "true");
		schema.getField("age").addProp("x-sort", "true");

		BinaryKey key = createKey("john", 30);
		avroKey.datum(key);
		partitioner.getPartition(avroKey, null, 2);
		
		assertSame(key.getBuffer(), partitioner.bytes);
		assertEquals(BinaryKey.KEY_BYTES_OFFSET, partitioner.byteOffset);
		
		assertEquals(0, key.groupBytesLength());
		assertTrue(key.keyBytesLength() > 0);
		assertEquals(key.keyBytesLength(), partitioner.byteLength);
	}
}
