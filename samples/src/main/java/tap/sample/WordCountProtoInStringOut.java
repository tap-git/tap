package tap.sample;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tap.core.*;
import tap.sample.WordCountProtobufInput.CountRec;

public class WordCountProtoInStringOut extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        /* Set up a basic pipeline of map reduce */
        Tap wordcount = new Tap(getClass()).named("wordcount");
        /* Parse options - just use the standard options - input and output location, time window, etc. */
        BaseOptions o = new BaseOptions();
        int result = o.parse(wordcount, args);
        if (result != 0)
            return result;
        if (o.input == null) {
            System.err.println("Must specify input directory");
            return 1;
        }
        if (o.output == null) {
            System.err.println("Must specify output directory");
            return 1;
        }

        Pipe input = new Pipe(o.input);
        Pipe<String> counts = new Pipe(o.output);
        wordcount.produces(counts);
        
        Phase count = new Phase().reads(input).writes(counts).map(Mapper.class).
            groupBy("word").reduce(Reducer.class);
        
        
        if (o.forceRebuild) wordcount.forceRebuild();
        if (o.dryRun) {
            wordcount.dryRun();
            return 0;
        }
        
        wordcount.execute();
        
        return 0;
    }

    public static class Mapper extends BaseMapper<Protos.CountRec,CountRec> {
        private static int i = 0;
    	
    	@Override
        public void map(Protos.CountRec in, CountRec out, TapContext<CountRec> context) {
        	
            out.word = in.getWord();
            out.count = 1;
            context.write(out);
            if (i % 100 == 0) {
            	System.out.println("Mapper Word=" + out.word + " Count=" + out.count);
            }
            i++;
        } 
    }

    public static class Reducer extends BaseReducer<CountRec,String> {

    	@Override
        public void reduce(Iterable<CountRec> in, String out, TapContext<String> context) {
            
            String word = null;
            int count = 0;
            for (CountRec rec : in) {
                if(word == null)
                    word = rec.word;
                count += rec.count;
            }
            
            context.write(word + "-X-" + count);

        }
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountProtoInStringOut(), args);
        System.exit(res);
    }

}
