/**
 * 
 */
package tap;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import tap.CommandOptions;
import tap.Phase;
import tap.Pipe;
import tap.Tap;
import tap.core.SummationMapper;
import tap.core.SummationPipeReducer;
import tap.core.WordCountMapper;
import tap.core.WordCountReducer;

/**
 * 
 */
public class BindingTests {
	
	@Test
	public void minimalistTest() {
		String args[] = { "BindingTests.mapOutTest", "-i",
				"share/decameron.txt", "-o", "/tmp/TapTestsOutput3", "--force" };

		Tap tap = new Tap(new CommandOptions(args));
		tap.createPhase().map(WordCountMapper.class).groupBy("word")
				.reduce(WordCountReducer.class);

		// to automatically trap Hadoop exceptions
		tap.alerter(new TapUnitTestAlerter());

		tap.make();
	}
	
	@Test
	public void fileBindingTest1() {
		String args2[] = { "BindingTests.mapOutTest", "-i", "/tmp/gaggle/", "-o",
				"/tmp/TapTestsOutput3", "--force" };
		CommandOptions o2 = new CommandOptions(args2);
		Tap tap2 = new Tap(o2);
		tap2.alerter(new TapUnitTestAlerter());
		
		Phase phase2 = tap2
				.createPhase().reads(o2.input)
				.map(SummationMapper.class).groupBy("word")
				.combine(SummationPipeReducer.class)
				.reduce(SummationPipeReducer.class).writes(o2.output);
		tap2.produces(phase2.getOutputs());
		phase2.plan(tap2);
		assertNotNull(phase2.input().getPath());
		System.out.println("timestamp " + phase2.input().getTimestamp());
		assertTrue(!phase2.input().exists());
		assertTrue(0 == phase2.input().getTimestamp());
		tap2.make();
	}

	@Test
	public void mapOutTest() {
		String args[] = { "BindingTests.mapOutTest", "-i", "share/decameron.txt", "-o",
				"/tmp/TapTestsOutput", "--force" };

		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);
		tap.alerter(new TapUnitTestAlerter());

		Phase phase1 = tap.createPhase()
				.reads(o.input)
				.map(WordCountMapper.class)
				.groupBy("word")
				.combine(WordCountReducer.class)
				.reduce(WordCountReducer.class)
				.writes(o.output);
		
		Assert.assertEquals(phase1.getInputs().get(0).getFormat().toString(), "STRING_FORMAT", phase1.getInputs().get(0).getFormat().toString());
		
		tap.produces(phase1.output());
		Assert.assertEquals(phase1.getInputs().get(0).getFormat().toString(), "STRING_FORMAT", phase1.getInputs().get(0).getFormat().toString());
		
		List<PhaseError> phaseErrors = phase1.plan(tap);
		Assert.assertNotNull(phaseErrors);
		Assert.assertEquals("Planning errors ", 0, phaseErrors.size());
		if (phaseErrors.size() > 0) {
			for(PhaseError e: phaseErrors) {
				System.out.println("mapOutTest: " + e.getMessage());
			}
		}
	    
		System.out.println(tap.getConf().get("mapred.output.format.class"));
		System.out.println(phase1.getOutputs().get(0).getFormat().toString());
		Assert.assertNotSame("UNKNOWN_FORMAT", phase1.getOutputs().get(0).getFormat().toString());
		
		Assert.assertEquals(phase1.getInputs().get(0).getFormat().toString(), 
				"STRING_FORMAT", 
				phase1.getInputs().get(0).getFormat().toString());
		Assert.assertEquals("AVRO_FORMAT", phase1.getOutputs().get(0).getFormat().toString());
		//tap.named(o.program).make();
	}
	
	@Test
	public void avroInputBindingTest() {
		
		{
			String args[] = { "BindingTests.mapOutTest", "-i",
					"share/decameron.txt", "-o", "/tmp/TapTestsOutput",
					"--force" };
			CommandOptions o1 = new CommandOptions(args);
			Tap tap1 = new Tap(o1);
			tap1.alerter(new TapUnitTestAlerter());

			Phase phase1 = tap1.createPhase().reads(o1.input)
					.map(WordCountMapper.class).groupBy("word")
					.combine(WordCountReducer.class)
					.reduce(WordCountReducer.class).writes(o1.output);

			tap1.named(o1.program).make();
		}
		{
		String args2[] = { "BindingTests.mapOutTest", "-i", "share/decameron.txt", "-o",
				"/tmp/TapTestsOutput2", "--force" };
		CommandOptions o2 = new CommandOptions(args2);
		Tap tap2 = new Tap(o2);
		tap2.alerter(new TapUnitTestAlerter());
		
		Phase phase2 = tap2
				.createPhase().reads(o2.input)
				.map(SummationMapper.class).groupBy("word")
				.combine(SummationPipeReducer.class)
				.reduce(SummationPipeReducer.class).writes(o2.output);
		
		//Assert.assertEquals("Avro input binding", 
		//		"AVRO_FORMAT", phase2.getInputs().get(0).getFormat().toString());
		
		tap2.produces(phase2.output());
		//Assert.assertEquals("AVRO_FORMAT", phase2.getInputs().get(0).getFormat().toString());
		
		Assert.assertEquals("Planning errors ", 0, phase2.plan(tap2).size());
	    
		System.out.println(tap2.getConf().get("mapred.output.format.class"));
		System.out.println(phase2.getOutputs().get(0).getFormat().toString());
		Assert.assertNotSame("UNKNOWN_FORMAT", phase2.getOutputs().get(0).getFormat().toString());
		Assert.assertEquals("AVRO_FORMAT", phase2.getOutputs().get(0).getFormat().toString());
		
		}
	}

	@Test
	public void outputStringBindingTest() {
		String args2[] = { "BindingTests.mapOutTest", "-i", "/tmp/TapTestsOutput", "-o",
				"/tmp/TapTestsOutput3", "--force" };
		CommandOptions o2 = new CommandOptions(args2);
		Tap tap2 = new Tap(o2);
		tap2.alerter(new TapUnitTestAlerter());
		
		Phase phase2 = tap2
				.createPhase().reads(o2.input)
				.map(SummationMapper.class).groupBy("word")
				.combine(SummationPipeReducer.class)
				.reduce(SummationPipeReducer.class).writes(o2.output);
		tap2.produces(phase2.getOutputs());
		phase2.plan(tap2);
		Assert.assertNotSame("STRING_FORMAT", phase2.getOutputs().get(0).getFormat().toString());
		Assert.assertEquals("AVRO_FORMAT", phase2.getInputs().get(0).getFormat().toString());
		
	}
	
	public class StringOutReducer extends TapReducer<CountRec, String> {

		private OutputLog outLog = new OutputLog("sum of words", 0);

		@Override
		public void reduce(Pipe<CountRec> in, Pipe<String> out) {
			int loopCount = 0;
			CountRec val;
			while (in.hasNext()) {
				val = in.next();
				// System.out.printf("<CountRec> (%s, %d) \n", val.word,
				// val.count);
				outLog.count += val.count;
				loopCount++;
			}
			System.out
					.printf("SumationPipeReducer: Loop Count=%d Outputing outlog.count = %d\n",
							loopCount, outLog.count);
			out.put("sum is " + outLog.count);
		}
	}

}
