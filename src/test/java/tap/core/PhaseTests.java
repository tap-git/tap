/**
 * 
 */
package tap.core;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Test;

import tap.formats.Formats;

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
		Pipe<String> input = new Pipe("input path.txt");
		Pipe<PhaseTests> output = new Pipe<PhaseTests>("outputpath.json");
		Phase phase = new Phase("my phase");
		phase.reads(input);
		phase.writes(output);
		Schema schema = Phase.getSchema(this);
		Assert.assertNotNull(schema);
		Assert.assertEquals("tap.core.PhaseTests", schema.getFullName());
		Assert.assertEquals(Type.RECORD,schema.getType());
		Assert.assertEquals(Formats.JSON_FORMAT,phase.getOutputs().get(0).getFormat());
		Assert.assertEquals(Formats.STRING_FORMAT, phase.getInputs().get(0).getFormat());
	}

}
