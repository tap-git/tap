package tap.core;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Date;
import java.util.StringTokenizer;

import org.joda.time.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tap.CommandOptions;
import tap.Phase;
import tap.Pipe;
import tap.Tap;
import tap.TapMapper;
import tap.TapReducer;
import tap.formats.tapproto.Testmsg;


public class MapOnlyTest {

	
	//@Test
	public void MapOnlyTest1() {
		String[] args = {"MaponlyTest1", "-i", "share/test_data.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().reads(o.input).map(MapperRecord1Record2.class);
		
		int rc = tap.make();
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.avro");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.

		
	}
	
	
	//@Test
	public void MapOnlyTest2() {
		String[] args = {"MaponlyTest2", "-i", "share/test_data.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().reads(o.input).map(MapperRecord1Record2.class);
		Phase phase2 = tap.createPhase().reads(phase1).map(MapperRecord2Record2.class).reduce(ReducerRecord2String.class).groupBy("extra2").writes(o.output);

		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.

		
	}
	
	@Test
	public void MapOnlyTest3() {
		String[] args = {"MaponlyTest3", "-i", "share/test_data.tapproto", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().reads(o.input).map(MapperTestRecordTestRecord.class);
	
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.tapproto");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.

		
	}
	
	@Test
	public void MapOnlyTest4() {
		String[] args = {"MaponlyTest4", "-i", "share/test_data.tapproto", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		Phase phase1 = tap.createPhase().reads(o.input).map(MapperTestRecordTestRecord.class);
		Phase phase2 = tap.createPhase().reads(phase1).map(MapperTestRecordTestRecord.class).reduce(ReducerTestRecordTestRecord.class).sortBy("group, extra, subsort").writes(o.output);
		int rc = tap.make();
		
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.tapproto");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.

		
	}
	
	
	
	public static class Record1{
        public String group;
        public String extra;
        public String subsort;
    }
	
	public static class Record2 
	{
		public String group2;
		public String extra2;
	}
		
	

	public static class MapperRecord1Record1 extends TapMapper<Record1, Record1> 
	{
		  static Record1 outrec = new Record1();
		  public void map(Record1 in, Pipe<Record1> out) {
			outrec.group = in.group;
			outrec.extra = in.extra;
			outrec.subsort = in.subsort;
			out.put(outrec);
			 
		  }
	}
	
	public static class MapperRecord2Record2 extends TapMapper<Record2, Record2>
	{
		static Record2 outrec = new Record2();
		 
		public void map(Record2 in, Pipe<Record2> out) {
			outrec.group2 = in.group2;
			outrec.extra2 = in.extra2;
			out.put(outrec);
		}
	}

	public static class MapperRecord1Record2 extends TapMapper<Record1, Record2>
	{
		static Record2 outrec= new Record2();
		public void map(Record1 in, Pipe<Record2> out)
		{
			outrec.group2 = in.group;
			outrec.extra2 = in.extra;
			out.put(outrec);
		}
	}
	
	

	public static class ReducerRecord1Record1 extends TapReducer<Record1, Record1>
	{

		static Record1 outrec = new Record1();
		
		 public void reduce(Pipe<Record1> in, Pipe<Record1> out) {
	        	
	            for (Record1 rec : in) {
	            	
	            	outrec.group = rec.group;
	            	outrec.extra = rec.extra;
	            	outrec.subsort = rec.subsort;
	                out.put(outrec);
	            }
		 }
	}

	public static class ReducerRecord1Record2 extends TapReducer<Record1, Record2>
	{

		static Record2 outrec = new Record2();
		
		 public void reduce(Pipe<Record1> in, Pipe<Record2> out) {
	        	
	            for (Record1 rec : in) {
	            	
	            	outrec.group2 = rec.group;
	            	outrec.extra2 = rec.extra;
	            	
	                out.put(outrec);
	            }
		 }
	}
	public static class ReducerRecord2Record2 extends TapReducer<Record2, Record2>
	{

		static Record2 outrec = new Record2();
		
		 public void reduce(Pipe<Record2> in, Pipe<Record2> out) {
	        	
	            for (Record2 rec : in) {
	            	
	            	outrec.group2 = rec.group2;
	            	outrec.extra2 = rec.extra2;
	            	
	                out.put(outrec);
	            }
		 }
	}
	
	

	
	
	public static class ReducerRecord2String extends TapReducer<Record2,String>
	{
		private String s;

        @Override
        public void reduce(Pipe<Record2> in, Pipe<String> out) {
        	
            for (Record2 rec : in) {
            	
            	s = new String(rec.group2 + " " + rec.extra2);
                out.put(s);
            }
          
        }
	}
	
	
	public static class MapperTestRecordTestRecord extends TapMapper<Testmsg.TestRecord, Testmsg.TestRecord>
	{
		public void map(Testmsg.TestRecord msg, Pipe<Testmsg.TestRecord> out)
		{
			out.put(msg);
		}
	}
	
	
	public static class ReducerTestRecordTestRecord extends TapReducer<Testmsg.TestRecord, Testmsg.TestRecord>
	{
		public void reduce(Pipe<Testmsg.TestRecord> in, Pipe<Testmsg.TestRecord> out)
		{
			for(Testmsg.TestRecord msg : in)
			{
				out.put(msg);
				
			}
		}
	}
}
