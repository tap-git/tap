/**
 * 
 */
package tap.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Test;

import tap.formats.Formats;
import tap.util.ReflectUtils;

public class PhaseTests {

    public String word;
    public int count;

    @Test
    public void constructor() {
        Phase phase = new Phase();
        Assert.assertNotNull(phase);
    }

    @Test
    public void schema() {
        Pipe<String> input = new Pipe("input path.txt");
        Pipe<PhaseTests> output = new Pipe<PhaseTests>("outputpath.json");
        Phase phase = new Phase("my phase");
        phase.reads(input);
        phase.writes(output);
        Schema schema = ReflectUtils.getSchema(this);
        Assert.assertNotNull(schema);
        Assert.assertEquals("tap.core.PhaseTests", schema.getFullName());
        Assert.assertEquals(Type.RECORD, schema.getType());
        Assert.assertEquals(Formats.JSON_FORMAT, phase.getOutputs().get(0)
                .getFormat());
        Assert.assertEquals(Formats.STRING_FORMAT, phase.getInputs().get(0)
                .getFormat());
    }
    
    @Test
    public void setNameTest() {
    	String[] args = {"Phase.setTest", "-i", "share/decameron.txt", "-o", "/tmp/out", "--force"};
    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
        Tap tap = new Tap(o);
        Phase phase = tap.createPhase();
        Assert.assertNotNull(phase.getName());
        phase.setName("my phase");
        Assert.assertNotNull(phase.getName());
    }
    
    @Test
    public void temporaryFileTest() {
    	String[] args = {"Phase.setTest", "-i", "share/decameron.txt", "-o", "/tmp/out", "--force"};
    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
        Tap tap = new Tap(o);
        Phase phase = tap
        .createPhase()
        .reads(o.input)
        .map(SetMapper.class)
        .reduce(SetReducer.class)
        //.combine(Test2Reducer.class)
        .groupBy("word")
        .writes(o.output);
        Assert.assertNotNull(phase.getTmpOutputName());
        System.out.println(phase.getTmpOutputName());
    }
    
    @Test
    public void temporaryFileTest2() {
    	String[] args = {"Phase.setTest", "-i", "share/decameron.txt", "-o", "/tmp/out", "--force"};
    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
		String tempFile = null;
    	for(int i=0; i< 2; i++) {
    		Tap t = new Tap(o);
    		Phase p = t.createPhase();
    		if (i==0) {
    			tempFile = p.getTmpOutputName();
    		} else {
    			Assert.assertEquals(tempFile, p.getTmpOutputName());
    		}
		}
    }

    @Test
    public void mapperSignatureTest() {
    	String[] args = {"mapperTest", "-i", "share/wordcount.out.avro", "-o", "/tmp/out", "--force"};
    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
        Tap tap = new Tap(o).named(o.program);

        Pipe<CountRec> p1 = new Pipe<CountRec>(o.input);
        p1.setPrototype(new CountRec());

        Pipe<OutputLog> p2 = new Pipe<OutputLog>(o.output);
        p2.setPrototype(new OutputLog());
        tap.produces(p2);
        

        Phase phase = new Phase();
        phase.reads(p1).writes(p2).map(Test2Mapper.class).groupBy("word")
                .reduce(Test2Reducer.class);
        phase.plan(tap);

        System.out.println(phase.getSummary());

        List<PhaseError> errors = phase.plan(tap);

        Assert.assertEquals(0, errors.size());

        Assert.assertNotNull("inputs missing", phase.getInputs().get(0));
        Assert.assertNotNull("outputs missing", phase.getOutputs().get(0));

        Assert.assertEquals("MAP IN CLASS", "tap.core.CountRec", phase
                .getConf().get(Phase.MAP_IN_CLASS));
        Assert.assertEquals("MAP OUT CLASS", "tap.core.CountRec", phase
                .getConf().get(Phase.MAP_OUT_CLASS));

    }
    
    @Test
    public void mapperSignatureTest2() {
    	String[] args = {"mapperTest", "-i", "share/wordcount.out.avro", "-o", "/tmp/out"};
    	CommandOptions o = new CommandOptions(args);
    	
        /* Set up a basic pipeline of map reduce */
        Tap assembly = new Tap(o).named("mapperTest");

        Pipe<CountRec> p1 = new Pipe("share/wordcount.out.avro");
        Pipe<OutputLog> p2 = new Pipe("/tmp/out");
        assembly.produces(p2);

        Phase phase = new Phase();
        phase.reads(p1).writes(p2).map(Test3Mapper.class).groupBy("word")
                .reduce(Test2Reducer.class);

        System.out.println(phase.getSummary());
        List<PhaseError> errors = phase.plan(assembly);

        Assert.assertEquals(0, errors.size());

        Assert.assertNotNull("inputs missing", phase.getInputs().get(0));
        Assert.assertNotNull("outputs missing", phase.getOutputs().get(0));

        Assert.assertEquals("MAP IN CLASS", String.class.getName(), phase
                .getConf().get(Phase.MAP_IN_CLASS));
        Assert.assertEquals("MAP OUT CLASS", "tap.core.CountRec", phase
                .getConf().get(Phase.MAP_OUT_CLASS));

    }


    @Test
    public void mapperSignatureTest3() {
    	String[] args = {"mapperTest", "-i", "share/wordcount.out.avro", "-o", "/tmp/out"};
    	CommandOptions o = new CommandOptions(args);
    	
        /* Set up a basic pipeline of map reduce */
        Tap assembly = new Tap(o).named("mapperTest");

        Pipe<String> p1 = new Pipe("share/decameron.txt");
        p1.setPrototype("prototype");

        Pipe<OutputLog> p2 = new Pipe<OutputLog>("/tmp/out");
        p2.setPrototype(new OutputLog());
        assembly.produces(p2);

        Phase phase = new Phase();
        phase.reads(p1).writes(p2).map(Test3Mapper.class).groupBy("count")
                .reduce(Test3Reducer.class);
        System.out.println(phase.getSummary());

        List<PhaseError> errors = phase.plan(assembly);
        for (PhaseError e : errors) {
            System.out.printf("%s : %s \n", e.getMessage(), (null == e.getException() ? "" : e.getException()
                    .toString()));
        }
        Assert.assertEquals(0, errors.size());

        Assert.assertNotNull("inputs missing", phase.getInputs().get(0));
        Assert.assertNotNull("outputs missing", phase.getOutputs().get(0));

        Assert.assertEquals("MAP IN class", String.class.getName(), phase
                .getConf().get(Phase.MAP_IN_CLASS));
        Assert.assertEquals("MAP OUT class", CountRec.class.getName(), phase
                .getConf().get(Phase.MAP_OUT_CLASS));
        Assert.assertEquals("REDUCER OUT class", OutputLog.class.getName(),
                phase.getConf().get(Phase.REDUCE_OUT_CLASS));
        Assert.assertEquals("Reducer", Test3Reducer.class.getName(), phase
                .getConf().get(Phase.REDUCER));

        assembly.dryRun();
    }

    @Test
	public void setTest() {
		String[] args = {"Phase.setTest", "-i", "share/decameron.txt", "-o", "/tmp/out", "--force"};
		CommandOptions o = new CommandOptions(args);
	    /* Set up a basic pipeline of map reduce */
	    Tap tap = new Tap(o);
	    HashMap<String, String> hash = new HashMap<String,String>();
	    hash.put("bob", "One");
	    hash.put("fruit", "apple");
	    hash.put("fruit", "Orange");
	    hash.put("animal", "dog");
	    
	    tap
	    .createPhase()
	    .reads(o.input)
	    .map(SetMapper.class)
	    .reduce(SetReducer.class)
	    //.combine(Test2Reducer.class)
	    .groupBy("word")
	    .writes(o.output)
	    .set("mykey", hash);
	    int rc = tap.named(args[0]).make();
	    Assert.assertEquals(0, rc);
	}

	public static class SetMapper extends TapMapper<String, CountRec> {
		private HashMap<String,String> myMapParam = null;
		
		@Override
		public void init(String Path) {
			Assert.assertNotNull(Path);
			Assert.assertEquals(true, Path.contains("decameron.txt"));
			if (null == myMapParam) {
				myMapParam = (HashMap<String, String>) this.getMapperParameter("mykey");
			}
			Assert.assertNotNull(myMapParam);
			System.out.println("SetMapper.init called");
		}
	
		private HashMap<String,String> getMyMap() {
			return myMapParam;
		}
	
		private CountRec outrec = new CountRec();
		@Override
		public void map(String in, Pipe<CountRec> out) {
			Assert.assertNotNull(getMyMap());
			Assert.assertEquals(3, getMyMap().size());
			Assert.assertEquals("dog", getMyMap().get("animal"));
			Assert.assertEquals("One", getMyMap().get("bob"));
			Assert.assertEquals("Orange", getMyMap().get("fruit"));
			outrec.word = in;
			outrec.count = 1;
			out.put(outrec);
		}
		
		@Override
		public void finish() {
			System.out.println("SetMapper.finish called");
		}
	}

	public static class SetReducer extends TapReducer<CountRec, OutputLog> {
	    
		@Override
		public void init(String path) {
			System.out.println("SetReducer.init("+path+") called");
		}
		
		@Override
	    public void reduce(Pipe<CountRec> in, Pipe<OutputLog> out) {
	    	
	    }
	    
	    @Override
	    public void finish() {
	    	System.out.println("SetReducer.finish called");
	    }
	
	}

	public class Test3Mapper extends TapMapper<String, CountRec> {
        @Override
        public void map(String in, Pipe<CountRec> out) {
            //TODO: Need a body here
        }
    }

    public class Test3Reducer extends TapReducer<CountRec, OutputLog> {
        @Override
        public void reduce(Pipe<CountRec> in, Pipe<OutputLog> out) {
        }
    }

    public class Test2Mapper extends TapMapper<CountRec, CountRec> {
        @Override
        public void map(CountRec in, Pipe<CountRec> out) {
            // preferred
        }

        public void map(Pipe<CountRec> in, Pipe<CountRec> out) {
            // not supported
        }

        public void map(double latitude, double longitude) {
            // not applicable
        }
    }

    public static class Test2Reducer extends TapReducer<CountRec, OutputLog> {
        @Override
        public void reduce(Pipe<CountRec> in, Pipe<OutputLog> out) {
        }
    }
}