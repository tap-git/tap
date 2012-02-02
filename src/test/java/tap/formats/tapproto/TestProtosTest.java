/**
 * 
 */
package tap.formats.tapproto;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

import tap.formats.tapproto.TestProtos.CountRecMessage;

/**
 * 
 */
public class TestProtosTest {

	@Test
	public void test() {
		CountRecMessage m = CountRecMessage.newBuilder().setCount(27).setWord("sum").build();
		Assert.assertNotNull(m);
		Assert.assertEquals(27, m.getCount());
		Assert.assertEquals("sum", m.getWord());
	}

}
