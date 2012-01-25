package tap.sample;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tap.core.Tap;
import tap.core.TapMapper;
import tap.core.CommandOptions;
import tap.core.TapReducer;
import tap.core.Pipe;
import tap.sample.WordCountProtobufInput.CountRec;

public class WordCountStringInStringGZOut extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

    	CommandOptions o = new CommandOptions(args);
        /* Set up a basic pipeline of map reduce */
        Tap wordcount = new Tap(o).named("wordcount");
    
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

    public static class Mapper extends TapMapper<String,CountRec> {
    	private CountRec outrec;
    	
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
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountStringInStringGZOut(), args);
        System.exit(res);
    }

}
