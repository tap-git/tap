package tap.core.io;

import java.io.ByteArrayOutputStream;

import org.junit.Before;
import org.junit.Test;
import junit.framework.Assert;

public class BinaryComparableOutputStreamTests {
	
	ByteArrayOutputStream array;
	BinaryComparableOutputStream output;

	@Before
	public void initialize() {
		array = new ByteArrayOutputStream();
		output = new BinaryComparableOutputStream(array);
	}
	
	@Test
	public void writeBoolAsc() {
	}
	
	@Test
	public void writeIntAsc() {
	}
	
	@Test
	public void writeLongAsc() {
	}
	
	@Test
	public void writeStringAsc() {
	}
	
	@Test
	public void writeFloatAsc() {
	}
	
	@Test
	public void writeDoubleAsc() {
	}
	
	@Test
	public void writeNullAsc() {
	}
	
	@Test
	public void writeRecordAsc() {
	}
	
	@Test
	public void writeBoolDesc() {
	}
	
	@Test
	public void writeIntDesc() {
	}
	
	@Test
	public void writeLongDesc() {
	}
	
	@Test
	public void writeStringDesc() {
	}
	
	@Test
	public void writeFloatDesc() {
	}
	
	@Test
	public void writeDoubleDesc() {
	}
	
	@Test
	public void writeNullDesc() {
	}
	
	@Test
	public void writeRecordDesc() {
	}
	
	@Test
	public void writeRecordAscDesc() {
		
	}
}
