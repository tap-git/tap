package tap.sample;

import tap.CommandOptions;
import tap.Pipe;
import tap.Tap;
import tap.core.*;
import tap.util.DefaultTimeShard;

public class SamplePipe {

    public static void main(String[] args) throws Exception {
       
        /* Parse options - just use the standard options - input and output location, time window, etc. */
        CommandOptions o = new CommandOptions(args);
        Tap pipeline = new Tap(o);

        String hrPath = new DefaultTimeShard().makePath(o.getStart());

        Pipe<LogRec> log = Pipe.of(LogRec.class).at("data/sample/logs/"+hrPath).jsonFormat();
        Pipe<LogRec> cleanLog = Pipe.of(LogRec.class).at("/tmp/conv/cleanlogs/"+hrPath).jsonFormat();
        pipeline.createPhase().reads(log).writes(cleanLog).map(LogClean.class).groupBy("cookie").sortBy("ts")
                .reduce(BotFilter.class).set("botLimit", "500");

        Pipe<LogRec> conversions = Pipe.of(LogRec.class).at("/tmp/conv/conversions/"+hrPath).jsonFormat();
        pipeline.createPhase().reads(cleanLog).writes(conversions).groupBy("cookie").sortBy("ts")
                .reduce(ConversionCount.class);
        
        pipeline.make();   
    }

}
