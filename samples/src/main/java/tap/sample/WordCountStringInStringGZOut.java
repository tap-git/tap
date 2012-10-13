package tap.sample;

import java.util.StringTokenizer;

import tap.CommandOptions;
import tap.Pipe;
import tap.Tap;
import tap.TapMapper;
import tap.TapReducer;
import tap.core.*;
import tap.sample.WordCountProtobufInput.CountRec;

public class WordCountStringInStringGZOut {

    public static void main(String[] args) throws Exception {

    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
        Tap wordcount = new Tap(o).named("wordcount");
    
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

    public static class Mapper extends TapMapper<String,CountRec> {
    	private CountRec outrec = new CountRec();
    	
        @Override
        public void map(String in, Pipe<CountRec> out) {
        	
    		StringTokenizer tokenizer = new StringTokenizer(in);
    		while (tokenizer.hasMoreTokens()) {
    			outrec.word = tokenizer.nextToken();
    			outrec.count = 1;
    			out.put(outrec);
    			System.out.println("Map Out " + outrec.word + " Count=" + outrec.count);
    		}
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
            System.out.println("Redcuer Out " + word + "-X-" + count);
            
            out.put(word + "-X-" + count);

        }
    }
    
}
