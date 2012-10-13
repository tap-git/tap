/**
 * 
 */
package tap.formats.tapproto;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

import tap.formats.tapproto.TestProtos.CountRecMessage;
import tap.util.ObjectFactory;

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
	
	@Test
	public void objectFactoryTest() throws Exception {
		CountRecMessage m = ObjectFactory.newInstance(CountRecMessage.class);
		Assert.assertNotNull(m);
		Assert.assertEquals(false, m.isInitialized());
		Assert.assertEquals(true, m.getAllFields().isEmpty());
		m.toBuilder().setCount(28).setWord("asdf").build();
		Assert.assertEquals(false, m.isInitialized());
		Assert.assertEquals(true, m.getAllFields().isEmpty());
		m = m.toBuilder().setCount(28).setWord("asdf").build();
		Assert.assertEquals(true, m.isInitialized());
		Assert.assertEquals(false, m.getAllFields().isEmpty());
		Assert.assertEquals("asdf", m.getWord());
		Assert.assertEquals(28,m.getCount());
	}
	
	

}
