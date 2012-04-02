/**
 * 
 */
package tap;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
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
import tap.formats.tapproto.Testmsg;

/**
 * 
 */
public class BindingTests {
	
	@Test
	public void fileBindingTest1() {
		String args2[] = { "BindingTests.fileBindingTest1", "-i", "/tmp/gaggle/", "-o",
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
	public void fileBindingTest2() throws IOException {
		String args2[] = { "BindingTests.fileBindingTest1", 
				"-i", "share/decameron.txt", 
				"-o", "/tmp/outfile.txt", "--force" };
		
		CommandOptions o = new CommandOptions(args2);
		Tap tap = new Tap(o);
		tap.alerter(new TapUnitTestAlerter());
		File f = new File(o.output);
		// touch the file
		if (f.exists())
		{
			// The file already exists, so just update its last modified time
			if (!f.setLastModified(System.currentTimeMillis()))
			{
				throw new IOException("Could not touch file");
			}
		}
		else
		{
			// The file doesn't exist, so create it
			f.createNewFile();
		}

		Phase phase2 = tap
				.createPhase()
				.map(WordCountMapper.class)
				.groupBy("word")
				.combine(WordCountReducer.class)
				.reduce(WordCountReducer.class)
				.reads(o.input)
				.writes(o.output);
		
		tap.produces(phase2.getOutputs());
		List<PhaseError> errors = phase2.plan(tap);
		//there are three errors, outfile.txt is not a directory and is is incompatible with the WordCountReducer out type and consequently there is no output type defined.
		Assert.assertEquals("Expecting output error", 1, errors.size());
		Assert.assertTrue(errors.get(0).getMessage().contains("should be a directory"));
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
		
		List<PhaseError> errors = phase2.plan(tap2);
		//there are and should be planning errors....the input is decameron.txt, but we are expecting AVRO.
		//Assert.assertEquals("Planning errors ", 0, errors.size());
	    
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
	
	@Test
	/*
	 * the input file contains data in avro format, mapper is expecting data in protobuf format.  phase.plan should generate an error.
	 */
	public void checkFileContentsTest() 
	{
		String args[] = { "BindingTests.checkFileContents", "-i", "share/test_data.avro", "-o",
				"/tmp/TapTestsOutput3", "--force" };
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);
		Phase phase = tap.createPhase().reads(o.input).map(Mapper.class).groupBy("group").reduce(Reducer.class).sortBy("extra, subsort").writes(o.output);
		tap.make();
		tap.produces(phase.getOutputs());
		List<PhaseError> phaseErrors = phase.plan(tap);
		Assert.assertNotNull(phaseErrors);
		Assert.assertTrue("planning error", phaseErrors.size() != 0);
	}
	
	@Test
	
	public void directoryTest()
	{
		String args[] = { "BindingTests.directoryTest", "-i",
				"share/multi/01", "-o", "/tmp/TapTestsOutput4", "--force" };
	
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);
		tap.createPhase().map(WordCountMapper.class).reads(o.input).groupBy("word")
				.reduce(WordCountReducer.class).writes(o.output);
	
		// to automatically trap Hadoop exceptions
		tap.alerter(new TapUnitTestAlerter());
	
		
		
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.avro");
        Assert.assertTrue(f.exists());
	}
	
	@Test
	public void wrongExtensionTest()
	{
		String args[] = { "BindingTests.checkFileContents", "-i", "share/wrong_extension.tapproto", "-o",
				"/tmp/TapTestsOutput3", "--force" };
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);
		Phase phase = tap.createPhase().reads(o.input).map(Mapper.class).groupBy("group").reduce(Reducer.class).sortBy("extra, subsort").writes(o.output);
		int rc = tap.make();
		Assert.assertFalse(rc == 0);
		
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
	
	public static class Mapper extends TapMapper<Testmsg.TestRecord, Testmsg.TestRecord>
	{
		public void map(Testmsg.TestRecord msg, Pipe<Testmsg.TestRecord> out)
		{
			out.put(msg);
		}
	}
	
	public static class Reducer extends TapReducer<Testmsg.TestRecord, Testmsg.TestRecord>
	{
		public void reduce(Pipe<Testmsg.TestRecord> in, Pipe<Testmsg.TestRecord> out)
		{
			System.out.println("**************");
			for(Testmsg.TestRecord rec : in)
			{
				System.out.println(rec.getGroup() + " " + rec.getExtra() + " " + rec.getSubsort());
			}
		}
	}

}
