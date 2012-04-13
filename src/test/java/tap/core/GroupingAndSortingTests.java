package tap.core;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import tap.*;
import tap.core.MapOnlyTest.Record1;
import tap.formats.tapproto.Testmsg;

public class GroupingAndSortingTests {

	@Test
	public void Test1() {
		String[] args = {"GroupingAndSortingTest1", "-i", "share/test_data2.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Record.class).reads(o.input).sortBy("group, extra, subsort").writes(o.output);
		int rc = tap.make();
		Assert.assertEquals(0, rc);
        File f1 = new File(o.output+"/part-00000.avro");
        File f2 = new File("share/results/sortby_group_extra_subsort.avro");
        Assert.assertTrue(f1.exists());
       
       //file compare doesn't work for avro files...date? 
       // Assert.assertTrue(f2.exists());
       // Assert.assertTrue(Utilities.fileContentsEquals(f1, f2));
        
		
	}
	
	@Test
	public void Test2() {
		String[] args = {"GroupingAndSortingTest2", "-i", "share/test_data2.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Record.class).reads(o.input).groupBy("group").sortBy("extra, subsort").writes(o.output);
		
		int rc = tap.make();
		Assert.assertEquals(0, rc);
		 File f1 = new File(o.output+"/part-00000.avro");
	     File f2 = new File("share/results/sortby_group_extra_subsort.avro");
	     Assert.assertTrue(f1.exists());
	    // Assert.assertTrue(f2.exists());
	    // Assert.assertTrue(Utilities.fileContentsEquals(f1, f2));
		
	}
	
	
	
	@Test
	public void Test3() {
		String[] args = {"GroupingAndSortingTest3", "-i", "share/test_data2.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Record.class).reads(o.input).reduce(Reducer.class).sortBy("group, extra, subsort").groupBy("group").writes(o.output);
		
		int rc = tap.make();
		Assert.assertEquals(0, rc);
		 File f1 = new File(o.output+"/part-00000.avro");
	     File f2 = new File("share/results/summation_on_group.avro");
	     Assert.assertTrue(f1.exists());
	     Assert.assertTrue(f2.exists());
	   // Assert.assertTrue(Utilities.fileContentsEquals(f1, f2));
		
	}
	
	@Test
	public void Test4() {
		String[] args = {"GroupingAndSortingTest4", "-i", "share/test_data.tapproto", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Testmsg.TestRecord.class).reads(o.input).reduce(Reducer2.class).groupBy("group, extra").writes(o.output);
		
		int rc = tap.make();
		Assert.assertEquals(0, rc);

		 File f1 = new File(o.output+"/part-00000.tapproto");
	     File f2 = new File("share/results/groupby_group_extra.tapproto");
	     Assert.assertTrue(f1.exists());
	     Assert.assertTrue(f2.exists());
	     Assert.assertTrue(Utilities.fileContentsEquals(f1, f2));
		
	}
	

	@Test
	public void Test5() {
		String[] args = {"GroupingAndSortingTest5", "-i", "share/securities_data.tapproto", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Testmsg.SecuritiesRecord.class).reads(o.input).reduce(Reducer3.class).sortBy("timestamp desc").writes(o.output);
		
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f1 = new File(o.output+"/part-00000.tapproto");
        File f2 = new File("share/results/sortby_timestamp_desc.tapproto");
        Assert.assertTrue(f1.exists());
        Assert.assertTrue(f2.exists());
        Assert.assertTrue(Utilities.fileContentsEquals(f1, f2));
    
     
		
	}
	
	@Test
	public void Test6() {
		String[] args = {"GroupingAndSortingTest5", "-i", "share/securities_data.tapproto", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().of(Testmsg.SecuritiesRecord.class).reads(o.input).reduce(Reducer3.class).groupBy("exchange").sortBy("id, timestamp").writes(o.output);
		
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
     
		File f1 = new File(o.output+"/part-00000.tapproto");
        File f2 = new File("share/results/groupby_exchange_sortby_id_timestamp.tapproto");
        Assert.assertTrue(f1.exists());
        Assert.assertTrue(f2.exists());
       Assert.assertTrue(Utilities.fileContentsEquals(f1, f2));
    
       
       

		
	}
	
	
	@Test
		public void Test7() {
			String[] args = {"GroupingAndSortingTest5", "-i", "share/securities_data.tapproto", "-o", "/tmp/out", "-f"};
			CommandOptions o = new CommandOptions(args);
			Tap tap = new Tap(o).named(o.program);
			
			Phase phase1 = tap.createPhase().of(Testmsg.SecuritiesRecord.class).reads(o.input).reduce(Reducer3.class).sortBy("exchange desc, strike").writes(o.output);
			
			int rc = tap.make();
			
			Assert.assertEquals(0, rc);
	    
			File f1 = new File(o.output+"/part-00000.tapproto");
	        File f2 = new File("share/results/sortby_exchange_desc_strike.tapproto");
	        Assert.assertTrue(f1.exists());
	        Assert.assertTrue(f2.exists());
	        Assert.assertTrue(Utilities.fileContentsEquals(f1, f2));
		}
	

	@Test
	public void Test8() {
			String[] args = {"GroupingAndSortingTest5", "-i", "share/securities_data.tapproto", "-o", "/tmp/out", "-f"};
			CommandOptions o = new CommandOptions(args);
			Tap tap = new Tap(o).named(o.program);
			
			Phase phase1 = tap.createPhase().map(Mapper.class).reads(o.input).reduce(Reducer3.class).groupBy("id").sortBy("expiry").writes(o.output);
			
			int rc = tap.make();
			
			Assert.assertEquals(0, rc);
	    
			File f1 = new File(o.output+"/part-00000.tapproto");
	        File f2 = new File("share/results/groupby_id_sortby_expiry.tapproto");
	        Assert.assertTrue(f1.exists());
	        Assert.assertTrue(f2.exists());
	        Assert.assertTrue(Utilities.fileContentsEquals(f1, f2));
	        
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
	//		System.out.println(outrec.group + " " + outrec.total);
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
				out.put(rec);
			}
		}
	}
	
	
	public static class Reducer3 extends  TapReducer<Testmsg.SecuritiesRecord, Testmsg.SecuritiesRecord>
	{
		public void reduce(Pipe<Testmsg.SecuritiesRecord> in, Pipe<Testmsg.SecuritiesRecord> out)
		{
			//System.out.println("************************");
			for(Testmsg.SecuritiesRecord rec : in)
			{
				//System.out.println(rec.getTimestamp() + " " + rec.getExchange() + " " + rec.getId() + " " + rec.getDesc() + " " + rec.getStrike() + " " + 
			//rec.getExpiry());
				out.put(rec);
			}
		}
		
	}

	
	
public static class Mapper extends TapMapper<Testmsg.SecuritiesRecord, Testmsg.SecuritiesRecord> 
{
		
		public void map(Testmsg.SecuritiesRecord msg, Pipe<Testmsg.SecuritiesRecord> out)
		{
			out.put(msg);
		}
	}

}
