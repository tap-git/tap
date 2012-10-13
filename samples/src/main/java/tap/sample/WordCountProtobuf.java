package tap.sample;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tap.CommandOptions;
import tap.Pipe;
import tap.Tap;
import tap.TapMapper;
import tap.TapReducer;
import tap.core.*;

public class WordCountProtobuf {

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
        
        /* new style mapping - does not work, check with Doug
        Phase count = new Phase().reads(input).writes(counts).map(PipeMapper.class).
            groupBy("word").reduce(PipeReducer.class);
        */
        wordcount.make();
    }

    public static class CountRec {
        public String word;
        public int count;
    }
    

    public static class Mapper extends TapMapper<String,CountRec> {
    	CountRec outrec = new CountRec();
 
        @Override
        public void map(String line, Pipe<CountRec> out) {
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                outrec.word = tokenizer.nextToken();
                outrec.count = 1;
                out.put(outrec);
            }
        }        
    }
    
    public static class PipeMapper extends TapMapper<String,CountRec> {
        @Override
        public void map(String in, Pipe<CountRec> out) {
            StringTokenizer tokenizer = new StringTokenizer(in);
           CountRec rec = new CountRec();
            while (tokenizer.hasMoreTokens()) {
                rec.word = tokenizer.nextToken();
                rec.count = 1;
                out.put(rec);
            }
        }        
    }
    

    public static class Reducer extends TapReducer<CountRec,Protos.CountRec> {
        
        // ProtobufWritable<Protos.CountRec> protoWritable = ProtobufWritable.newInstance(Protos.CountRec.class);
        
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
    
    public static class PipeReducer extends TapReducer<CountRec,Protos.CountRec> {
        
        // ProtobufWritable<Protos.CountRec> protoWritable = ProtobufWritable.newInstance(Protos.CountRec.class);
        
        @Override
        public void reduce(Pipe<CountRec> in, Pipe<Protos.CountRec> out) {
            
            String word = null;
            int count = 0;
            for (CountRec rec : in) {
                if(word == null)
                    word = rec.word;
                count += rec.count;
            }
            
            Protos.CountRec rec = Protos.CountRec.newBuilder()
                    .setWord(word)
                    .setCount(count)
                    .build();
            out.put(rec);
        }
    }
}
