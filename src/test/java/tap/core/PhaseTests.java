/**
 * 
 */
package tap.core;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.apache.avro.Schema;
import org.junit.Test;

/**
 * @author dmoore-apple
 *
 */

public class PhaseTests {

	public String word;
	public int count;
	
	@Test
	public void constructor() {
		Phase phase = new Phase();
		Assert.assertNotNull(phase);
	}
	
	@Test
	public void schema() {
		Pipe<String> input = new Pipe("input path");
		Pipe<PhaseTests> output = new Pipe<PhaseTests>("output path");
		Phase p = new Phase("my phase");
		p.reads(input);
		p.writes(output);
		Schema schema = Phase.getSchema(this);
		Assert.assertNotNull(schema);
	}

}
