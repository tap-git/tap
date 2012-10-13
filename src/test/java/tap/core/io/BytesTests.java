package tap.core.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class BytesTests {
	
	ByteArrayOutputStream out;
	
	@Before
	public void setup() {
		out = new ByteArrayOutputStream();
	}
	
	@Test
	public void writeEmptyString() throws IOException {
		int len = Bytes.writeString("", out, SortOrder.ASCENDING);
		byte[] bytes = out.toByteArray();
		Assert.assertEquals(1, len);
		Assert.assertEquals(len, bytes.length);
		Assert.assertEquals(0, bytes[0]);
	}
	
	@Test
	public void writeEmptyStringDesc() throws IOException {
		int len = Bytes.writeString("", out, SortOrder.DESCENDING);
		byte[] bytes = out.toByteArray();
		Assert.assertEquals(1, len);
		Assert.assertEquals(len, bytes.length);
		Assert.assertEquals(Bytes.UBYTE_MAX_VALUE, bytes[0] & 0xff);
	}
	
	@Test
	public void writeStringEscape() throws IOException {
		String val = new String(new byte[] { 0, 1 });
		int len = Bytes.writeString(val, out, SortOrder.ASCENDING);
		byte[] bytes = out.toByteArray();
		byte[] expected = new byte[] { Bytes.ESC, 0, Bytes.ESC, 1, Bytes.TERM };
		
		Assert.assertEquals(5, len);
		Assert.assertEquals(len, bytes.length);
		Assert.assertEquals(0, Bytes.compare(expected, bytes));
	}

	@Test
	public void writeStringEmbeddedEscape() throws IOException {
		String val = new String(new byte[] { 'a', Bytes.ESC, 'b', 'c', Bytes.TERM, 'd' });
		int len = Bytes.writeString(val, out, SortOrder.ASCENDING);
		byte[] bytes = out.toByteArray();
		byte[] expected = new byte[] {
			'a', Bytes.ESC, Bytes.ESC, 'b', 'c', Bytes.ESC, Bytes.TERM, 'd', Bytes.TERM
		};
		
		Assert.assertEquals(9, len);
		Assert.assertEquals(len, bytes.length);
		Assert.assertEquals(0, Bytes.compare(expected, bytes));
	}

	@Test
	public void writeStringEscapeDesc() throws IOException {
		String val = new String(new byte[] { 0, 1 });
		int len = Bytes.writeString(val, out, SortOrder.DESCENDING);
		byte[] bytes = out.toByteArray();
		byte[] expected = new byte[] {
			(byte) Bytes.UBYTE_MAX_VALUE - Bytes.ESC,
			(byte) Bytes.UBYTE_MAX_VALUE - 0,
			(byte) Bytes.UBYTE_MAX_VALUE - Bytes.ESC,
			(byte) Bytes.UBYTE_MAX_VALUE - 1,
			(byte) Bytes.UBYTE_MAX_VALUE - Bytes.TERM
		};
		
		Assert.assertEquals(5, len);
		Assert.assertEquals(len, bytes.length);
		Assert.assertEquals(0, Bytes.compare(expected, bytes));
	}
	
	@Test
	public void writeString() throws IOException {
		int len = Bytes.writeString("abc", out, SortOrder.ASCENDING);
		byte[] bytes = out.toByteArray();
		byte[] expected = new byte[] { 'a', 'b', 'c', Bytes.TERM };
		
		Assert.assertEquals(4, len);
		Assert.assertEquals(len, bytes.length);
		Assert.assertEquals(0, Bytes.compare(expected, bytes));
	}
	
	@Test
	public void compareStringAsc() throws IOException {
		byte[] b1 = Bytes.getBytes("abc", SortOrder.ASCENDING);
		
		byte[] b2 = Bytes.getBytes("abc", SortOrder.ASCENDING);
		Assert.assertEquals(0, Bytes.compare(b1, b2));
		
		b2 = Bytes.getBytes("ab", SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b1, b2) > 0);
		
		b2 = Bytes.getBytes("abd", SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b1, b2) < 0);
	}
	
	@Test
	public void compareStringDesc() throws IOException {
		byte[] b1 = Bytes.getBytes("abc", SortOrder.DESCENDING);
		
		byte[] b2 = Bytes.getBytes("abc", SortOrder.DESCENDING);
		Assert.assertEquals(0, Bytes.compare(b1, b2));
		
		b2 = Bytes.getBytes("ab", SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b1, b2) < 0);
		
		b2 = Bytes.getBytes("abd", SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b1, b2) > 0);
	}
	
	@Test
	public void booleanAscNotSameAsDesc() {
		byte[] b1 = Bytes.getBytes(true, SortOrder.ASCENDING);
		byte[] b2 = Bytes.getBytes(true, SortOrder.DESCENDING);
		
		Assert.assertEquals(1, b1.length);
		Assert.assertEquals(1, b2.length);
		
		Assert.assertFalse(Bytes.compare(b1, b2) == 0);
	}
	
	@Test
	public void writeBoolean() {
		byte[] b1 = Bytes.getBytes(true, SortOrder.ASCENDING);
		byte[] b2 = Bytes.getBytes(false, SortOrder.ASCENDING);
		
		Assert.assertTrue(Bytes.toBoolean(b1, 0, SortOrder.ASCENDING));
		Assert.assertFalse(Bytes.toBoolean(b2, 0, SortOrder.ASCENDING));
		
		b1 = Bytes.getBytes(true, SortOrder.DESCENDING);
		b2 = Bytes.getBytes(false, SortOrder.DESCENDING);
		
		Assert.assertTrue(Bytes.toBoolean(b1, 0, SortOrder.DESCENDING));
		Assert.assertFalse(Bytes.toBoolean(b2, 0, SortOrder.DESCENDING));
	}
	
	@Test
	public void compareBoolean() {
		byte[] b1 = Bytes.getBytes(false, SortOrder.ASCENDING);
		byte[] b2 = Bytes.getBytes(true, SortOrder.ASCENDING);
		
		Assert.assertTrue(Bytes.compare(b1, b2) < 0);
		
		b1 = Bytes.getBytes(false, SortOrder.DESCENDING);
		b2 = Bytes.getBytes(true, SortOrder.DESCENDING);
		
		Assert.assertTrue(Bytes.compare(b1, b2) > 0);
	}

	
	@Test
	public void intAscNotSameAsDesc() {
		byte[] b1 = Bytes.getBytes(0, SortOrder.ASCENDING);
		byte[] b2 = Bytes.getBytes(0, SortOrder.DESCENDING);
		
		Assert.assertEquals(4, b1.length);
		Assert.assertEquals(4, b2.length);
		
		Assert.assertFalse(Bytes.compare(b1, b2) == 0);
	}
	
	@Test
	public void writeIntAsc() {
		byte[] bytes = Bytes.getBytes(0, SortOrder.ASCENDING);
		Assert.assertEquals(0, Bytes.toInt(bytes, 0, SortOrder.ASCENDING));
		
		bytes = Bytes.getBytes(1, SortOrder.ASCENDING);
		Assert.assertEquals(1, Bytes.toInt(bytes, 0, SortOrder.ASCENDING));
		
		bytes = Bytes.getBytes(-1, SortOrder.ASCENDING);
		Assert.assertEquals(-1, Bytes.toInt(bytes, 0, SortOrder.ASCENDING));
		
		bytes = Bytes.getBytes(Integer.MIN_VALUE, SortOrder.ASCENDING);
		Assert.assertEquals(Integer.MIN_VALUE, Bytes.toInt(bytes, 0, SortOrder.ASCENDING));
		
		bytes = Bytes.getBytes(Integer.MAX_VALUE, SortOrder.ASCENDING);
		Assert.assertEquals(Integer.MAX_VALUE, Bytes.toInt(bytes, 0, SortOrder.ASCENDING));
	}

	@Test
	public void writeIntDesc() {
		byte[] bytes = Bytes.getBytes(0, SortOrder.DESCENDING);
		Assert.assertEquals(0, Bytes.toInt(bytes, 0, SortOrder.DESCENDING));
		
		bytes = Bytes.getBytes(1, SortOrder.DESCENDING);
		Assert.assertEquals(1, Bytes.toInt(bytes, 0, SortOrder.DESCENDING));
		
		bytes = Bytes.getBytes(-1, SortOrder.DESCENDING);
		Assert.assertEquals(-1, Bytes.toInt(bytes, 0, SortOrder.DESCENDING));
		
		bytes = Bytes.getBytes(Integer.MIN_VALUE, SortOrder.DESCENDING);
		Assert.assertEquals(Integer.MIN_VALUE, Bytes.toInt(bytes, 0, SortOrder.DESCENDING));
		
		bytes = Bytes.getBytes(Integer.MAX_VALUE, SortOrder.DESCENDING);
		Assert.assertEquals(Integer.MAX_VALUE, Bytes.toInt(bytes, 0, SortOrder.DESCENDING));
	}
	
	@Test
	public void compareIntAsc() {
		byte[] b1 = Bytes.getBytes(Integer.MIN_VALUE, SortOrder.ASCENDING);
		
		byte[] b2 = Bytes.getBytes(-1, SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b1, b2) < 0);
		
		byte[] b3 = Bytes.getBytes(0, SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b2, b3) < 0);
		
		byte[] b4 = Bytes.getBytes(1, SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b3, b4) < 0);
		
		byte[] b5 = Bytes.getBytes(Integer.MAX_VALUE, SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b4, b5) < 0);
		
		Assert.assertTrue(Bytes.compare(b1, b5) < 0);
	}
	
	@Test
	public void compareIntDesc() {
		byte[] b1 = Bytes.getBytes(Integer.MIN_VALUE, SortOrder.DESCENDING);
		
		byte[] b2 = Bytes.getBytes(-1, SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b1, b2) > 0);
		
		byte[] b3 = Bytes.getBytes(0, SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b2, b3) > 0);
		
		byte[] b4 = Bytes.getBytes(1, SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b3, b4) > 0);
		
		byte[] b5 = Bytes.getBytes(Integer.MAX_VALUE, SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b4, b5) > 0);
		
		Assert.assertTrue(Bytes.compare(b1, b5) > 0);
	}
	
	
	@Test
	public void longAscNotSameAsDesc() {
		byte[] b1 = Bytes.getBytes(0L, SortOrder.ASCENDING);
		byte[] b2 = Bytes.getBytes(0L, SortOrder.DESCENDING);
		
		Assert.assertEquals(8, b1.length);
		Assert.assertEquals(8, b2.length);
		
		Assert.assertFalse(Bytes.compare(b1, b2) == 0);
	}
	
	@Test
	public void writeLongAsc() {
		byte[] bytes = Bytes.getBytes(0L, SortOrder.ASCENDING);
		Assert.assertEquals(0L, Bytes.toLong(bytes, 0, SortOrder.ASCENDING));
		
		bytes = Bytes.getBytes(1L, SortOrder.ASCENDING);
		Assert.assertEquals(1L, Bytes.toLong(bytes, 0, SortOrder.ASCENDING));
		
		bytes = Bytes.getBytes(-1L, SortOrder.ASCENDING);
		Assert.assertEquals(-1L, Bytes.toLong(bytes, 0, SortOrder.ASCENDING));
		
		bytes = Bytes.getBytes(Long.MIN_VALUE, SortOrder.ASCENDING);
		Assert.assertEquals(Long.MIN_VALUE, Bytes.toLong(bytes, 0, SortOrder.ASCENDING));
		
		bytes = Bytes.getBytes(Long.MAX_VALUE, SortOrder.ASCENDING);
		Assert.assertEquals(Long.MAX_VALUE, Bytes.toLong(bytes, 0, SortOrder.ASCENDING));
	}

	@Test
	public void writeLongDesc() {
		byte[] bytes = Bytes.getBytes(0L, SortOrder.DESCENDING);
		Assert.assertEquals(0L, Bytes.toLong(bytes, 0, SortOrder.DESCENDING));
		
		bytes = Bytes.getBytes(1L, SortOrder.DESCENDING);
		Assert.assertEquals(1L, Bytes.toLong(bytes, 0, SortOrder.DESCENDING));
		
		bytes = Bytes.getBytes(-1L, SortOrder.DESCENDING);
		Assert.assertEquals(-1L, Bytes.toLong(bytes, 0, SortOrder.DESCENDING));
		
		bytes = Bytes.getBytes(Long.MIN_VALUE, SortOrder.DESCENDING);
		Assert.assertEquals(Long.MIN_VALUE, Bytes.toLong(bytes, 0, SortOrder.DESCENDING));
		
		bytes = Bytes.getBytes(Long.MAX_VALUE, SortOrder.DESCENDING);
		Assert.assertEquals(Long.MAX_VALUE, Bytes.toLong(bytes, 0, SortOrder.DESCENDING));
	}

	@Test
	public void compareLongAsc() {
		byte[] b1 = Bytes.getBytes(Long.MIN_VALUE, SortOrder.ASCENDING);
		
		byte[] b2 = Bytes.getBytes(-1L, SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b1, b2) < 0);
		
		byte[] b3 = Bytes.getBytes(0L, SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b2, b3) < 0);
		
		byte[] b4 = Bytes.getBytes(1L, SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b3, b4) < 0);
		
		byte[] b5 = Bytes.getBytes(Long.MAX_VALUE, SortOrder.ASCENDING);
		Assert.assertTrue(Bytes.compare(b4, b5) < 0);
		
		Assert.assertTrue(Bytes.compare(b1, b5) < 0);
	}
	
	
	@Test
	public void compareLongDesc() {
		byte[] b1 = Bytes.getBytes(Long.MIN_VALUE, SortOrder.DESCENDING);
		
		byte[] b2 = Bytes.getBytes(-1L, SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b1, b2) > 0);
		
		byte[] b3 = Bytes.getBytes(0L, SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b2, b3) > 0);
		
		byte[] b4 = Bytes.getBytes(1L, SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b3, b4) > 0);
		
		byte[] b5 = Bytes.getBytes(Long.MAX_VALUE, SortOrder.DESCENDING);
		Assert.assertTrue(Bytes.compare(b4, b5) > 0);
		
		Assert.assertTrue(Bytes.compare(b1, b5) > 0);
	}

	
	@Test
	public void compareSame() {
		byte[] b1 = "abc".getBytes();
		byte[] b2 = "abc".getBytes();
		
		Assert.assertEquals(0, Bytes.compare(b1, b2));
	}
	
	@Test
	public void compareDiff() {
		byte[] b1 = "a".getBytes();
		byte[] b2 = "b".getBytes();
		
		Assert.assertTrue(Bytes.compare(b1, b2) < 0);
		Assert.assertTrue(Bytes.compare(b2, b1) > 0);
	}
	
	@Test
	public void compareDiffLength() {
		byte[] b1 = "a".getBytes();
		byte[] b2 = "ab".getBytes();
		
		Assert.assertTrue(Bytes.compare(b1, b2) < 0);
		Assert.assertTrue(Bytes.compare(b2, b1) > 0);
	}
}
