
package tap.sample;

import java.util.StringTokenizer;
import tap.*;
import CountRec; 

public class WordCount extends Tap {
    // note that Tap.main can be used, it can use
    // System.getProperty("sun.java.command") to find the class you ACTUALLY wanted to run
    // a bit hacky, but feasible
    @Override
    public int run(CommandOptions o) throws Exception {
        // this version only has to parse the Tap-specific CommandOptions, not Hadoop generic ones like libjars
        // Hadoop generic ones get parsed by Tap and hidden from applications
        newPhase()
            .reads(o.input).map(WordCountMapper.class).combine(WordCountReducer.class)
            .groupBy("word")
            .writes(o.output).reduce(WordCountReducer.class);
        return make();
    }

    public static class WordCountMapper extends TapMapper {
        @Override
        public void map(String in, Pipe<CountRec> out) {
            StringTokenizer tokenizer = new StringTokenizer(in);
            while (tokenizer.hasMoreTokens()) {
                // wouldn't it be better to just setWord and only build once per item
                // a downside of protobuf is the need to generate 1 output object per record,
                // unlike POJOs where you can reuse the same object
                out.put(CountRec.newBuilder().setWord(tokenizer.nextToken()).setCount(1).build()));
            }
        }
    }

    public static class WordCountReducer extends TapReducer {
        @Override
        public void reduce(Pipe<CountRec> in, Pipe<CountRec> out) {
            String word = null;
            int count = 0;
            for (CountRec rec : in) {
                if (word == null) word = rec.getWord();
                count += rec.getCount();
            }
            out.put(CountRec.newBuilder().setWord(word).setCount(count).build());
        }
    }
}