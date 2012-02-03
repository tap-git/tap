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
