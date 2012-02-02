package tap.formats;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;
import tap.formats.avro.AvroFormat;

public class AvroFormatCheckTests {

	@Test
	public void isInstanceOfTest() {
		
		AvroFormat af = new AvroFormat();

		Assert.assertTrue(af.instanceOfCheck(MyStruct.class));
		Assert.assertTrue(af.instanceOfCheck(new MyStruct()));
		
		Assert.assertFalse(af.instanceOfCheck(new String("")));
//		Assert.assertFalse(af.instanceOfCheck(String.class));
		
	}
	
	public class MyStruct {
		int id;
		String name;
	}

}
