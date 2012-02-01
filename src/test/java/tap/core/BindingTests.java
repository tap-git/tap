/**
 * 
 */
package tap.core;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class BindingTests {
	
	@Test
	public void mapOutTest() {
		String args[] = { "BindingTests.mapOutTest", "-i", "/tmp/TapTests/maugham.txt", "-o",
				"/tmp/TapTestsOutput", "--force" };

		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);

		Phase phase1 = tap.createPhase()
				.reads(o.input)
				.map(WordCountMapper.class)
				.groupBy("word")
				.combine(WordCountReducer.class)
				.reduce(WordCountReducer.class)
				.writes(o.output);
		tap.produces(phase1.output());
		Assert.assertEquals(0, phase1.plan(tap).size());
		phase1.plan(tap);
	    
		System.out.println(tap.getConf().get("mapred.output.format.class"));
		System.out.println(phase1.getOutputs().get(0).getFormat().toString());
		Assert.assertNotSame("UNKNOWN_FORMAT", phase1.getOutputs().get(0).getFormat().toString());
		Assert.assertEquals("AVRO_FORMAT", phase1.getOutputs().get(0).getFormat().toString());
		//tap.named(o.program).make();
	}

}
