/**
 * 
 */
package tap.core;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import tap.CommandOptions;
import tap.Phase;
import tap.Pipe;
import tap.Tap;
import tap.TapReducer;
import tap.Utilities;
import tap.core.WordCountReducer;
import tap.core.GroupingAndSortingTests.Reducer3;
import tap.formats.tapproto.Testmsg;
/**
 *
 */
public class ChainingTests {

	@Test
	public void chainTestNoForce() {
		String[] args = {"Phase.chainTest", "-i", "share/decameron.txt", "-o", "/tmp/chaintestsout"};
		CommandOptions o = new CommandOptions(args);
		File f = null;
		for (int i = 0; i < 2; i++) {
			/* Set up a basic pipeline of map reduce */
			Tap tap = new Tap(o).named(o.program);
	
			Assert.assertNotNull(tap);
			Phase phase1 = tap.createPhase().reads(o.input)
					.map(WordCountMapper.class).groupBy("word")
					.reduce(WordCountReducer.class);
	
			@SuppressWarnings("unchecked")
			Phase phase2 = tap.createPhase().reads(phase1)
					.map(SummationMapper.class).groupBy("word")
					.reduce(SummationReducer.class).writes(o.output);
			int rc = tap.make();
			
	        Assert.assertEquals(0, rc);
	        f = new File(o.output+"/part-00000.avro");
	        System.out.println(f.length());
	        Assert.assertTrue(f.exists());
	        Assert.assertTrue("length < 500", 500 > f.length());
	        if (0 == i) {
				try {
					System.out.println("Sleeping");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	    Date d = new Date(); // NOW
	    long timediff = d.getTime() - f.lastModified();
	    Assert.assertTrue("Was not created recently " + timediff, timediff > 5000);
	}

	@Test
	public void chainTest() {
		String[] args = {"Phase.chainTest", "-i", "share/decameron.txt", "-o", "/tmp/chaintestsout", "--force"};
    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
        Tap tap = new Tap(o).named(o.program);
        
        Assert.assertNotNull(tap);
        
       
        Phase phase1 = tap.createPhase()
        	.reads(o.input)
        	.map(WordCountMapper.class)
        	.groupBy("word")
        	.reduce(WordCountReducer.class);
        
        Phase phase2 = tap
        	.createPhase()
        	.reads(phase1)
        	.map(SummationMapper.class)
        	.groupBy("word")
        	.reduce(SummationReducer.class)
        	.writes(o.output);        	
        int rc = tap.make();
        Assert.assertEquals("make's return code", 0, rc);
        File f = new File(o.output+"/part-00000.avro");
        Assert.assertTrue(f.exists());
        Assert.assertTrue("length < 500", 500 > f.length());
        Date d = new Date(); // NOW
        long timediff = d.getTime() - f.lastModified();
        System.out.println(timediff);
        Assert.assertTrue("Created recently", timediff > 0);
        Assert.assertTrue(timediff < 20000);
	}
	
	@Test
	public void chainTestNoForceTouchInitial() {
		
		{
			String[] args = {"Phase.chainTest", "-i", "share/decameron.txt", "-o", "/tmp/chaintestsout"};
			CommandOptions o = new CommandOptions(args);
			/* Set up a basic pipeline of map reduce */
			Tap tap = new Tap(o).named(o.program);

			Assert.assertNotNull(tap);

			Phase phase1 = tap.createPhase().reads(o.input)
					.map(WordCountMapper.class).groupBy("word")
					.reduce(WordCountReducer.class);

			Phase phase2 = tap.createPhase().reads(phase1)
					.map(SummationMapper.class).groupBy("word")
					.reduce(SummationReducer.class).writes(o.output);
			int rc = tap.make();
			
	        Assert.assertEquals(0, rc);
	        File f = new File(o.output+"/part-00000.avro");
	        System.out.println(f.length());
	        Assert.assertTrue(f.exists());
	        Assert.assertTrue("length < 500", 500 > f.length());
	        Date d = new Date(); // NOW
	        long timediff = d.getTime() - f.lastModified();
	        System.out.println(timediff);

	        Assert.assertTrue("Created recently", timediff > 0);
	        Assert.assertTrue("Was created recently " + timediff, timediff < 10000);
		}
        
        /**
         * Touch the input file
         */
        File f1 = new File("share/decameron.txt");
        Assert.assertEquals(true, f1.exists());
        f1.setLastModified(new Date().getTime()); // touch the file
        
		{
			String[] args = {"Phase.chainTest", "-i", "share/decameron.txt", "-o", "/tmp/chaintestsout"};
			CommandOptions o = new CommandOptions(args);
			/* Set up a basic pipeline of map reduce */
			Tap tap = new Tap(o).named(o.program);

			Assert.assertNotNull(tap);

			Phase phase1 = tap.createPhase().reads(o.input)
					.map(WordCountMapper.class).groupBy("word")
					.reduce(WordCountReducer.class);

			Phase phase2 = tap.createPhase().reads(phase1)
					.map(SummationMapper.class).groupBy("word")
					.reduce(SummationReducer.class).writes(o.output);
			int rc = tap.make();
		}
	}
	
	@Test
	public void tapfileChainTest()
	{
		
			String[] args = {"tapfileChainTest", "-i", "share/securities_data.tapproto", "-o", "/tmp/chaintestsout", "-f"};
			CommandOptions o = new CommandOptions(args);
			Tap tap = new Tap(o).named(o.program);
			
			Phase phase1 = tap.createPhase().of(Testmsg.SecuritiesRecord.class).reads(o.input).reduce(Reducer.class).sortBy("timestamp desc");
			Phase phase2 = tap.createPhase().reads(phase1).of(Testmsg.SecuritiesRecord.class).reduce(Reducer.class).sortBy("id").writes(o.output);
			int rc = tap.make();
			
			Assert.assertEquals(0, rc);
	    
			File f1 = new File(o.output+"/part-00000.tapproto");
	        Assert.assertTrue(f1.exists());
	   

	}
	
	public static class Reducer extends  TapReducer<Testmsg.SecuritiesRecord, Testmsg.SecuritiesRecord>
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
	
}
