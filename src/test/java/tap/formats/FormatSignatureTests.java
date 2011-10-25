/**
 * 
 */
package tap.formats;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import tap.formats.avro.AvroFormat;
import java.io.FileInputStream;
/**
 * Test File format signatures
 * @author Douglas Moore
 *
 */
public class FormatSignatureTests {

	@Test
	public void testAvroFileSignature() {
		FileFormat format = new AvroFormat();
		byte[] header = {0x4F,0x62,0x6A,0x01};
		boolean result = format.signature(header);
		Assert.assertTrue("file signature match",result);
	}
	
	@Test
	public void testAvroRealFileSignature() {
		String path = "/tmp/out/part.avro";
		byte header[] = new byte[100];
		try {
			FileInputStream fis = new FileInputStream(path);
			fis.read(header, 100, 0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
