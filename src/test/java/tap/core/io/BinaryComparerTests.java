package tap.core.io;

import org.junit.Assert;
import org.junit.Test;

public class BinaryComparerTests {
	
	@Test
	public void compareSame() {
		byte[] b1 = "abc".getBytes();
		byte[] b2 = "abc".getBytes();
		
		Assert.assertEquals(0, BinaryComparer.compareBytes(b1, b2));
	}
	
	@Test
	public void compareDiff() {
		byte[] b1 = "a".getBytes();
		byte[] b2 = "b".getBytes();
		
		Assert.assertTrue(BinaryComparer.compareBytes(b1, b2) < 0);
		Assert.assertTrue(BinaryComparer.compareBytes(b2, b1) > 0);
	}
	
	@Test
	public void compareDiffLength() {
		byte[] b1 = "a".getBytes();
		byte[] b2 = "ab".getBytes();
		
		Assert.assertTrue(BinaryComparer.compareBytes(b1, b2) < 0);
		Assert.assertTrue(BinaryComparer.compareBytes(b2, b1) > 0);
	}
}
