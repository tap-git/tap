package tap.sample;

import tap.core.*;
import tap.sample.WordCountProtobufInput.CountRec;

public class WordCountProtoInStringOut  {

    public static int main(String[] args) throws Exception {
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
        
        wordcount.make();
        
        return 0;
    }

    public static class Mapper extends TapMapper<Protos.CountRec,CountRec> {
        private static int i = 0;
        private CountRec outrec = new CountRec();
    	
    	@Override
        public void map(Protos.CountRec in, Pipe<CountRec> out) {
        	
            outrec.word = in.getWord();
            outrec.count = 1;
            out.put(outrec);
            if (i % 100 == 0) {
            	System.out.println("Mapper Word=" + outrec.word + " Count=" + outrec.count);
            }
            i++;
        } 
    }

    public static class Reducer extends TapReducer<CountRec,String> {

    	@Override
        public void reduce(Pipe<CountRec> in, Pipe<String> out) {
            
            String word = null;
            int count = 0;
            for (CountRec rec : in) {
                if(word == null)
                    word = rec.word;
                count += rec.count;
            }
            out.put(word + "-X-" + count);
       }
    }
}
