/**
 * 
 */
package tap.formats;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;

import junit.framework.Assert;

import org.junit.Test;

import tap.formats.avro.AvroFormat;
import tap.formats.json.JsonFormat;
import tap.formats.tapproto.TapprotoFormat;
import tap.formats.text.TextFormat;

/**
 * Test File format signatures
 * 
 * @author Douglas Moore
 * 
 */
public class FormatSignatureTests {

	@Test
	public void testAvroFileSignature() {
		FileFormat format = new AvroFormat();
		byte[] header = { 0x4F, 0x62, 0x6A, 0x01 };
		boolean result = format.signature(header);
		Assert.assertTrue("file signature match", result);
	}

	@Test
	public void testTapprotoFileSignature() {
		FileFormat format = new TapprotoFormat();
		byte[] header = "tapproto".getBytes();
		boolean result = format.signature(header);
		Assert.assertTrue("file signature match", result);
	}

	@Test
	public void testJsonFileSignature() {
		FileFormat format = new JsonFormat();
		byte[] header = " { blah blah blah { }\n }\n".getBytes();
		boolean result = format.signature(header);
		Assert.assertTrue("file signature match", result);
	}

	@Test
	public void testTextFileSignature() {
		FileFormat format = new TextFormat();
		byte[] header = "The quick brown dog jumped over the fox.\n Mary had a little lamb\n"
				.getBytes();
		boolean result = format.signature(header);
		Assert.assertTrue("file signature match", result);
	}

	@Test
	public void testNegativeTextFileSignature1() {
		byte[] header = "The quick brown dog jumped over the fox. Mary had a little lamb"
				.getBytes();
		FileFormat format = new TextFormat();
		boolean result = format.signature(header);
		Assert.assertFalse("file signature match", result);
	}

	@Test
	public void testNegativeTextFileSignature2() {
		byte[] header = "The quick brown dog jumped  \0x07 over the fox.\n Mary had a little lamb"
				.getBytes();
		FileFormat format = new TextFormat();
		boolean result = format.signature(header);
		Assert.assertFalse("file signature match", result);
	}

	@Test
	public void testNegativeTextFileSignatureUnicode() {
		byte[] header = "	\u0000The quick brown dog jumped over the fox.\n Mary had a little lamb"
				.getBytes();
		FileFormat format = new TextFormat();
		boolean result = format.signature(header);
		Assert.assertFalse("file signature match", result);
	}

	/*
	 * 4F 62 6A 01
	 */
	@Test
	public void testAvroRealFileSignature() {
		String path = "/tmp/out/part-00000.avro";
		byte header[] = new byte[100];
		try {
			FileInputStream fis = new FileInputStream(path);
			fis.read(header, 0, 100);
			fis.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BigInteger bi = new BigInteger(1, header);
		System.out.printf("len %d  value %s ", header.length,
				String.format("%0" + (header.length << 1) + "X", bi));

		Assert.assertTrue("header length", (header.length > 4));
		FileFormat format = new AvroFormat();
		boolean result = format.signature(header);
		Assert.assertTrue("file signature match ", result);

	}
}