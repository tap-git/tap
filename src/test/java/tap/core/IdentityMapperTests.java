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


public class IdentityMapperTests {

	
	@Test
	public void IdentityMapIdentityReduce() {
		String[] args = {"IdentityMapper", "-i", "share/test_data.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		tap.createPhase().reads(o.input).of(key.class).sortBy("group extra").writes(o.output);
		
		tap.make();
		
	}
	
	
	@Test
	public void IdentityReducer() {
		String[] args = {"IdentityMapper", "-i", "share/test_data.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		tap.createPhase().reads(o.input).map(Mapper.class).of(key2.class).sortBy("group2").writes(o.output);
		
		tap.make();
		
	}
	
	
	@Test
	public void IdentityReducer2() {
		String[] args = {"IdentityMapper", "-i", "share/decameron.txt", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		tap.createPhase().reads(o.input).map(Mapper2.class).of(key2.class).sortBy("group2").writes(o.output);
		
		tap.make();
		
	}
	
	
	@Test
	public void IdentityMapper() {
		String[] args = {"IdentityMapper", "-i", "share/test_data.avro", "-o", "/tmp/out", "-f"};
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o).named(o.program);
		tap.createPhase().reads(o.input).of(key.class).reduce(Reducer.class).groupBy("group").sortBy("extra, subsort").writes(o.output);
		
		tap.make();
		
	}
	
	public static class key {
        public String group;
        public String extra;
        public String subsort;
    }
	
	public static class key2 
	{
		public String group2;
		public String extra2;
	}
		

	public static class Mapper2 extends TapMapper<String, key2>
	{
		  static key2 outrec = new key2();
		  
		  public void map(String line, Pipe<key2> out)
		  {
			  StringTokenizer tokenizer = new StringTokenizer(line);
			  while (tokenizer.hasMoreTokens()) {
				  outrec.group2 = tokenizer.nextToken();
				  outrec.extra2 = "zzz";
				  out.put(outrec);
			  }
          }
	}
	
	public static class Mapper extends TapMapper<key, key2> 
	{
		  static key2 outrec = new key2();
		  public void map(key in, Pipe<key2> out) {
			outrec.group2 = in.group;
			outrec.extra2 = in.extra;
			out.put(outrec);
			 
		  }
	}
	
	public static class Reducer extends TapReducer<key,key>
	{
		private key outrec = new key();

        @Override
        public void reduce(Pipe<key> in, Pipe<key> out) {
        	//System.out.println("**************");
            for (key rec : in) {
            	
            	outrec.group = rec.group;
            	outrec.extra = rec.extra;
            	outrec.subsort = rec.subsort;
            	//System.out.println("REDUCER::" + outrec.group + " " + outrec.extra + " " + outrec.subsort);
                out.put(outrec);
            }
          
        }
	}
	
}
