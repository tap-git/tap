/**
 * 
 */
package tap.core;

import java.util.List;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Test;

import tap.formats.Formats;

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
        Schema schema = Phase.getSchema(this);
        Assert.assertNotNull(schema);
        Assert.assertEquals("tap.core.PhaseTests", schema.getFullName());
        Assert.assertEquals(Type.RECORD, schema.getType());
        Assert.assertEquals(Formats.JSON_FORMAT, phase.getOutputs().get(0)
                .getFormat());
        Assert.assertEquals(Formats.STRING_FORMAT, phase.getInputs().get(0)
                .getFormat());
    }

    @Test
    public void mapperSignatureTest() {
        /* Set up a basic pipeline of map reduce */
        Assembly assembly = new Assembly(getClass()).named("mapperTest");

        Pipe<CountRec> p1 = new Pipe<CountRec>("share/wordcount.out.avro");
        p1.setPrototype(new CountRec());

        Pipe<OutputLog> p2 = new Pipe<OutputLog>("/tmp/out");
        p2.setPrototype(new OutputLog());
        assembly.produces(p2);

        Phase phase = new Phase();
        phase.reads(p1).writes(p2).map(Test2Mapper.class).groupBy("word")
                .reduce(Test2Reducer.class);

        System.out.println(phase.getSummary());

        List<PhaseError> errors = phase.plan(assembly);

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
        /* Set up a basic pipeline of map reduce */
        Assembly assembly = new Assembly(getClass()).named("mapperTest");

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
        /* Set up a basic pipeline of map reduce */
        Assembly assembly = new Assembly(getClass()).named("mapperTest");

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
            System.out.printf("%s : %s \n", e.getMessage(), e.getException()
                    .toString());
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

    public class Test3Mapper extends BaseMapper<String, CountRec> {
        @Override
        public void map(String in, CountRec out, TapContext<CountRec> context) {
            context.write(out);
        }
    }

    public class Test3Reducer extends BaseReducer<CountRec, OutputLog> {
        @Override
        public void reduce(Pipe<CountRec> in, Pipe<OutputLog> out) {
        }
    }

    public class Test2Mapper extends BaseMapper<CountRec, CountRec> {
        @Override
        public void map(CountRec in, Pipe<CountRec> out) {
            // preferred
        }

        @Override
        public void map(CountRec in, CountRec out, TapContext<CountRec> context) {
            // legacy
        }

        public void map(Pipe<CountRec> in, Pipe<CountRec> out) {
            // not supported
        }

        public void map(double latitude, double longitude) {
            // not applicable
        }
    }

    public class Test2Reducer extends BaseReducer<CountRec, OutputLog> {
        @Override
        public void reduce(Pipe<CountRec> in, Pipe<OutputLog> out) {
        }
    }
}