package tap.core;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import tap.*;
import tap.core.MapOnlyTest.Record1;
import tap.formats.tapproto.Testmsg;

public class GroupingAndSortingTests {

	/*
	@Test
	public void Test1() {
		String[] args = {"GroupingAndSortingTest1", "-i", "share/test_data2.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Record.class).reads(o.input).sortBy("group, extra, subsort");
		
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.avro");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.

		
	}
	
	@Test
	public void Test2() {
		String[] args = {"GroupingAndSortingTest2", "-i", "share/test_data2.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Record.class).reads(o.input).groupBy("group").sortBy("extra, subsort");
		
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.avro");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.

		
	}
	
	
	
	@Test
	public void Test3() {
		String[] args = {"GroupingAndSortingTest3", "-i", "share/test_data2.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Record.class).reads(o.input).reduce(Reducer.class).sortBy("group, extra, subsort").groupBy("group");
		
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.avro");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.

		
	}
	
	@Test
	public void Test4() {
		String[] args = {"GroupingAndSortingTest4", "-i", "share/test_data.tapproto", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Testmsg.TestRecord.class).reads(o.input).reduce(Reducer2.class).groupBy("group, extra").sortBy("subsort");
		
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.tapproto");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.

		
	}
	*/
	
	@Test
	/*
	 * nb can not put enums in the key for now
	 */
	public void Test5() {
		String[] args = {"GroupingAndSortingTest5", "-i", "share/securities_data.tapproto", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Testmsg.SecuritiesRecord.class).reads(o.input).reduce(Reducer3.class).sortBy("timestamp, exchange, id");
		
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.tapproto");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.

		
	}
	
	
	public static class Record {
        public String group;
        public String extra;
        public String subsort;
        public int value;
    }
	
	public static class SummaryRecord
	{
		public String group;
		public int total;
	}
	
	public static class Reducer extends TapReducer<Record, SummaryRecord>  
	{
		static SummaryRecord outrec = new SummaryRecord();
		
		public void reduce(Pipe<Record> in, Pipe<SummaryRecord> out)
		{
			outrec.total = 0;
			
			for(Record rec : in)
			{
				outrec.group = rec.group;
				outrec.total++;
				
			}
			out.put(outrec);
			System.out.println(outrec.group + " " + outrec.total);
		}
	}
	
	
	public static class Reducer2 extends TapReducer<Testmsg.TestRecord, Testmsg.TestRecord>
	{
		public void reduce(Pipe<Testmsg.TestRecord> in, Pipe<Testmsg.TestRecord> out)
		{
			//System.out.println("**************");
			for(Testmsg.TestRecord rec : in)
			{
				//System.out.println(rec.getGroup() + " " + rec.getExtra() + " " + rec.getSubsort());
			}
		}
	}
	
	
	public static class Reducer3 extends  TapReducer<Testmsg.SecuritiesRecord, Testmsg.SecuritiesRecord>
	{
		public void reduce(Pipe<Testmsg.SecuritiesRecord> in, Pipe<Testmsg.SecuritiesRecord> out)
		{
			for(Testmsg.SecuritiesRecord rec : in)
			{
				//System.out.println(rec.getTimestamp() + " " + rec.getExchange() + " " + rec.getId() + " " + rec.getDesc() + " " + rec.getStrike() + " " + 
			//rec.getExpiry());
				
			}
		}
		
	}
	
}
