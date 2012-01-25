package tap.sample;
import tap.core.*;
//Wolf
public class WordCountAvroInput {

    public int main(String[] args) throws Exception {
    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
        Tap wordcount = new Tap(o).named("wordcount");
        /* Parse options - just use the standard options - input and output location, time window, etc. */
        
        if (o.input == null) {
            System.err.println("Must specify input directory");
            return 1;
        }
        if (o.output == null) {
            System.err.println("Must specify output directory");
            return 1;
        }

        wordcount.createPhase().reads(o.input).writes(o.output).map(Mapper.class).
            groupBy("word").reduce(Reducer.class);
        
        return wordcount.make();
       
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
