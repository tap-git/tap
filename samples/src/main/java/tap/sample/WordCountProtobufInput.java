package tap.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tap.core.*;

public class WordCountProtobufInput extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
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

    public static class CountRec {
        public String word;
        public int count;
    }
    

    public static class Mapper extends TapMapper<Protos.CountRec,CountRec> {
    	CountRec outRec;
        @Override
        public void map(
                Protos.CountRec in,
                Pipe<CountRec> out) {
            this.outRec.word = in.getWord();
            this.outRec.count = 1;
            out.put(this.outRec);
        } 
    }

    public static class Reducer extends TapReducer<CountRec,Protos.CountRec> {
        
        @Override
        public void reduce(Pipe<CountRec> in, Pipe<Protos.CountRec> out) {
            
            String word = null;
            int count = 0;
            for (CountRec rec : in) {
                if(word == null)
                    word = rec.word;
                count += rec.count;
            }
            
            out.put(Protos.CountRec.newBuilder()
                    .setWord(word)
                    .setCount(count)
                    .build());
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountProtobufInput(), args);
        System.exit(res);
    }

}
