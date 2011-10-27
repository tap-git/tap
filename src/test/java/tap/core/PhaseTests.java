/**
 * 
 */
package tap.core;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Test;

import tap.formats.Formats;

/**
 * @author dmoore-apple
 * 
 */

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
        phase.plan(assembly);

        Assert.assertNotNull("inputs missing", phase.getInputs().get(0));
        Assert.assertNotNull("outputs missing", phase.getOutputs().get(0));
    }

    public class Test2Mapper extends BaseMapper<CountRec, CountRec> {
        public void map(CountRec in, Pipe<CountRec> out) {
        }
    }

    public class Test2Reducer extends BaseReducer<CountRec, OutputLog> {
        public void reduce(Pipe<CountRec> in, Pipe<OutputLog> out) {
        }
    }
}
