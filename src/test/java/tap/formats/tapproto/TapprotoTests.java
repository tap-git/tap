package tap.formats.tapproto;



import java.io.File;
import java.util.List;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.junit.Test;

import tap.CommandOptions;
import tap.CountRec;
import tap.Phase;
import tap.PhaseError;
import tap.Pipe;
import tap.Tap;
import tap.TapMapper;
import tap.TapReducer;
import tap.core.WordCountMapper;
import tap.core.WordCountReducer;
import tap.formats.Formats;
import tap.formats.tapproto.Testmsg;
import tap.formats.tapproto.Testmsg.TestMsg;

public class TapprotoTests {

	
	@Test
	public void Test1() {
		
		String args[] = { "TapprotoTest1", "-i", "share/test_data.avro", "-o",
				"/tmp/out", "--force" };

		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);
		
		tap.createPhase().reads(o.input).map(MapperAvroTapproto.class).sortBy("group, extra, subsort").writes(o.output);
		
		int rc = tap.make();
		Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.tapproto");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        //should compare against pre-defined output.
	}
	

	
	
	
	@Test
	public void Test2()
	{
		String args[] = { "TapProtoMapReduceTest", "-i", "share/test_data.tapproto", "-o",
				"/tmp/out", "--force" };
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);
		
		tap.createPhase().reads(o.input).map(MapperTapprotoTapproto.class).reduce(ReducerTapprotoTapproto.class).sortBy("subsort, extra").writes(o.output);
		
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
    }
	
	
	
	public static class MapperTapprotoTapproto extends TapMapper<Testmsg.TestRecord, Testmsg.TestRecord>
	{
		public void map(Testmsg.TestRecord msg, Pipe<Testmsg.TestRecord> out)
		{
			out.put(msg);
		}
	}
	
	
	public static class MapperAvroTapproto extends TapMapper<Record, Testmsg.TestRecord>
	{
		public void map(Record avro_record, Pipe<Testmsg.TestRecord> out)
		{
			
			Testmsg.TestRecord record = Testmsg.TestRecord.newBuilder().setGroup(avro_record.group).setExtra(avro_record.extra).setSubsort(avro_record.subsort).build();
			out.put(record);
		}
	}
	
	
	
	
	public static class ReducerTapprotoTapproto extends TapReducer<Testmsg.TestRecord, Testmsg.TestRecord>
	{
		public void reduce(Pipe<Testmsg.TestRecord> in, Pipe<Testmsg.TestRecord> out)
		{
			for(Testmsg.TestRecord msg : in)
			{
				out.put(msg);
				//System.out.println(msg.getGroup() + " " + msg.getExtra() + " " + msg.getSubsort());
			}
		}
	}
}
