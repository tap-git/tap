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


public class IdentityTests {

	
	@Test
	public void Test1() {
		String[] args = {"IdentityTest1", "-i", "share/test_data.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		//use an identity mapper and identity reducer.
		tap.createPhase().reads(o.input).of(Record1.class).sortBy("group extra").writes(o.output);
		
		int rc = tap.make();
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.avro");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.
		
		
		
	}
	
	
	@Test
	public void Test2() {
		String[] args = {"IdentityTest2", "-i", "share/test_data.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		
		//identity reducer
		tap.createPhase().reads(o.input).map(MapperRecord1Record2.class).sortBy("group2").writes(o.output);
		
		int rc = tap.make();
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.avro");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.
		
	}
	
	
	@Test
	public void Test3() {
		String[] args = {"IdentityMapper", "-i", "share/test_data.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		//identity mapper
		tap.createPhase().reads(o.input).of(Record1.class).reduce(ReducerRecord1Record1.class).groupBy("group").sortBy("extra, subsort").writes(o.output);
		

		int rc = tap.make();
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.avro");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.
		
		
	}
	
	
	
	
	
	public static class Record1 {
        public String group;
        public String extra;
        public String subsort;
    }
	
	public static class Record2 
	{
		public String group2;
		public String extra2;
	}
		

	
	
	public static class MapperRecord1Record2 extends TapMapper<Record1, Record2> 
	{
		  static Record2 outrec = new Record2();
		  public void map(Record1 in, Pipe<Record2> out) {
			outrec.group2 = in.group;
			outrec.extra2 = in.extra;
			out.put(outrec);
			 
		  }
	}
	
	public static class ReducerRecord1Record1 extends TapReducer<Record1,Record1>
	{
		private Record1 outrec = new Record1();

        @Override
        public void reduce(Pipe<Record1> in, Pipe<Record1> out) {
        	
            for (Record1 rec : in) {
            	
            	outrec.group = rec.group;
            	outrec.extra = rec.extra;
            	outrec.subsort = rec.subsort;
                out.put(outrec);
            }
          
        }
	}
	
}
