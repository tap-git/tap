package tap.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tap.core.*;
import tap.util.DefaultTimeShard;

public class SamplePipe extends Configured implements Tool {

    @SuppressWarnings({ "unchecked", "unused" })
    @Override
    public int run(String[] args) throws Exception {
        Assembly pipe = new Assembly(getClass());

        /* Parse options - just use the standard options - input and output location, time window, etc. */
        BaseOptions o = new BaseOptions();
        int result = o.parse(pipe, args);
        if (result != 0)
            return result;

        String hrPath = new DefaultTimeShard().makePath(o.getStart());

        Pipe<LogRec> log = Pipe.of(LogRec.class).at("data/sample/logs/"+hrPath).jsonFormat();
        Pipe<LogRec> cleanLog = Pipe.of(LogRec.class).at("/tmp/conv/cleanlogs/"+hrPath).jsonFormat();
        Phase clean = new Phase().reads(log).writes(cleanLog).map(LogClean.class).groupBy("cookie").sortBy("ts")
                .reduce(BotFilter.class).set("botLimit", "500");

        Pipe<LogRec> conversions = Pipe.of(LogRec.class).at("/tmp/conv/conversions/"+hrPath).jsonFormat();
        Phase convert = new Phase().reads(cleanLog).writes(conversions).groupBy("cookie").sortBy("ts")
                .reduce(ConversionCount.class);
        
        pipe.produces(conversions);
        

        if (Boolean.TRUE.equals(o.forceRebuild)) pipe.forceRebuild();
        pipe.execute();
        return 0;        
    }
    
    //move this Hadoop dependency
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SamplePipe(), args);
        System.exit(res);
    }
}
