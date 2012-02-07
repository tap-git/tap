package tap.sample;
import tap.CommandOptions;
import tap.Pipe;
import tap.Tap;
import tap.TapMapper;
import tap.TapReducer;
import tap.core.*;
//Wolf
public class WordCountAvroInput {

    public static void main(String[] args) throws Exception {
    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
        Tap wordcount = new Tap(o).named("wordcount");
        /* Parse options - just use the standard options - input and output location, time window, etc. */
        
        if (o.input == null) {
            System.err.println("Must specify input directory");
            return;
        }
        if (o.output == null) {
            System.err.println("Must specify output directory");
            return;
        }

        wordcount.createPhase().reads(o.input).writes(o.output).map(Mapper.class).
            groupBy("word").reduce(Reducer.class);
        
        wordcount.make();
    }

    public static class CountRec {
        public String word;
        public int count;
    }
    

    public static class Mapper extends TapMapper<CountRec,CountRec> {
        @Override
        public void map(CountRec in, Pipe<CountRec> out) {
            out.put(in);
        }        
    }

    public static class Reducer extends TapReducer<CountRec,CountRec> {
    	private CountRec outrec = new CountRec();

        @Override
        public void reduce(Pipe<CountRec> in, Pipe<CountRec> out) {
        	outrec.count = 0;
            for (CountRec rec : in) {
            	outrec.word = rec.word;
            	outrec.count += rec.count;
            }
            out.put(outrec);
        }
        
    }
  
}
