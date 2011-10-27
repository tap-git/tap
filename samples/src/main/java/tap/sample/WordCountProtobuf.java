package tap.sample;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import tap.core.*;

public class WordCountProtobuf extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        /* Set up a basic pipeline of map reduce */
        Assembly wordcount = new Assembly(getClass()).named("wordcount");
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

        Pipe input = new Pipe(o.input).stringFormat();
        Pipe counts = Pipe.of(Protos.CountRec.class).at(o.output).protoFormat();
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

    public static class CountRec {
        public String word;
        public int count;
    }
    

    public static class Mapper extends BaseMapper<String,CountRec> {
        @Override
        public void map(String line, CountRec out, TapContext<CountRec> context) {
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                out.word = tokenizer.nextToken();
                out.count = 1;
                context.write(out);
            }
        }        
    }

    public static class Reducer extends BaseReducer<CountRec,Protos.CountRec> {
        
        // ProtobufWritable<Protos.CountRec> protoWritable = ProtobufWritable.newInstance(Protos.CountRec.class);
        
        @Override
        public void reduce(Iterable<CountRec> in, Protos.CountRec out, TapContext<Protos.CountRec> context) {
            
            String word = null;
            int count = 0;
            for (CountRec rec : in) {
                if(word == null)
                    word = rec.word;
                count += rec.count;
            }
            
            out = Protos.CountRec.newBuilder()
                    .setWord(word)
                    .setCount(count)
                    .build();
            context.write(out);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountProtobuf(), args);
        System.exit(res);
    }

}
