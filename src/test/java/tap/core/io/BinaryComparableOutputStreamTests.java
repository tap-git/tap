package tap.core.io;

import java.io.ByteArrayOutputStream;

import org.junit.Before;
import org.junit.Test;
import junit.framework.Assert;

public class BinaryComparableOutputStreamTests {
	
	ByteArrayOutputStream array;
	BinaryComparableOutputStream output;

	@Before
	void initialize() {
		array = new ByteArrayOutputStream();
		output = new BinaryComparableOutputStream(array);
	}
	
	@Test
	public void writeBoolAsc(boolean value, SortOrder order) {
	}
	
	@Test
	public void writeIntAsc(int value, SortOrder order) {
	}
	
	@Test
	public void writeLongAsc(long value, SortOrder order) {
	}
	
	@Test
	public void writeStringAsc(String value, SortOrder order) {
	}
	
	@Test
	public void writeFloatAsc(float value, SortOrder order) {
	}
	
	@Test
	public void writeDoubleAsc(double value, SortOrder order) {
	}
	
	@Test
	public void writeNullAsc(SortOrder order) {
	}
	
	@Test
	public void writeRecordAsc() {
	}
	
	@Test
	public void writeBoolDesc(boolean value, SortOrder order) {
	}
	
	@Test
	public void writeIntDesc(int value, SortOrder order) {
	}
	
	@Test
	public void writeLongDesc(long value, SortOrder order) {
	}
	
	@Test
	public void writeStringDesc(String value, SortOrder order) {
	}
	
	@Test
	public void writeFloatDesc(float value, SortOrder order) {
	}
	
	@Test
	public void writeDoubleDesc(double value, SortOrder order) {
	}
	
	@Test
	public void writeNullDesc(SortOrder order) {
	}
	
	@Test
	public void writeRecordDesc() {
	}
	
	@Test
	public void writeRecordAscDesc() {
		
	}
}
