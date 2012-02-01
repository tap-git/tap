/**
 * 
 */
package tap.core;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import tap.core.WordCountReducer;
/**
 *
 */
public class ChainingTests {

	@Test
	public void chainTest() {
		String[] args = {"Phase.chainTest", "-i", "share/decameron.txt", "-o", "/tmp/out", "--force"};
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
        Assert.assertEquals(0, rc);
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
	public void chainTestNoForce() {
		String[] args = {"Phase.chainTest", "-i", "share/decameron.txt", "-o", "/tmp/out"};
    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
        Tap tap = new Tap(o).named(o.program);
        
        Assert.assertNotNull(tap);
       
        try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
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
        Assert.assertEquals(0, rc);
        File f = new File(o.output+"/part-00000.avro");
        System.out.println(f.length());
        Assert.assertTrue(f.exists());
        Assert.assertTrue("length < 500", 500 > f.length());
        Date d = new Date(); // NOW
        long timediff = d.getTime() - f.lastModified();
        System.out.println(timediff);

        Assert.assertTrue("Created recently", timediff > 0);
        Assert.assertTrue("Was not created recently " + timediff, timediff > 5000);
	}
	
	@Test
	public void chainTestNoForceTouchInitial() {
		
		{
			String[] args = {"Phase.chainTest", "-i", "share/decameron.txt", "-o", "/tmp/out"};
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
			String[] args = {"Phase.chainTest", "-i", "share/decameron.txt", "-o", "/tmp/out"};
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
}
