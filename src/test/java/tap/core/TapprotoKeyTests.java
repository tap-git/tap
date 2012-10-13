package tap.core;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import tap.CommandOptions;
import tap.Phase;
import tap.Pipe;
import tap.Tap;
import tap.TapReducer;
import tap.Utilities;

import tap.formats.tapproto.Testmsg;


public class TapprotoKeyTests {
	
	
	@Test
	public void Test1() {
		String[] args = {"TapprotoKeyTests", "-i", "share/test_data.tapproto", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		 //key should only contain group
		 tap.createPhase().of(Testmsg.TestRecord.class).reads(o.input).reduce(Reducer.class).groupBy("group, extra").writes(o.output);
		
		 int rc = tap.make();
		 Assert.assertEquals(0, rc);

		
		
	}
	@Test
	public void Test2() {
		String[] args = {"TapprotoKeyTests", "-i", "share/test_data.tapproto", "-o", "/tmp/out3", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		 //no reducer but groupby specified, so key should be group, extra
		 tap.createPhase().reads(o.input).groupBy("group, extra").writes(o.output);
		
		 int rc = tap.make();
		 Assert.assertEquals(0, rc);

		
		
	}
	
	@Test
	public void Test3() {
		String[] args = {"TapprotoKeyTests", "-i", "share/test_data.tapproto", "-o", "/tmp/out4", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		//key should be group, subsort
		 tap.createPhase().reads(o.input).reduce(Reducer.class).sortBy("group, subsort, extra").writes(o.output);
		
		 int rc = tap.make();
		 Assert.assertEquals(0, rc);

		
		
	}
	
	
	public static class Reducer extends TapReducer<Testmsg.TestRecord, Testmsg.TestRecordSubset>
	{
		Testmsg.TestRecordSubset msg;
		public void reduce(Pipe<Testmsg.TestRecord> in, Pipe<Testmsg.TestRecordSubset> out)
		{
			for(Testmsg.TestRecord rec : in)
			{
				msg = Testmsg.TestRecordSubset.newBuilder().setGroup(rec.getGroup()).setSubsort(rec.getSubsort()).build();
				out.put(msg);
			}
		}
	}
	
}
