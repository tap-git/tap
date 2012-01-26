package tap.sample;
import tap.*;
public class PureSort {
    public static int main(String[] args) throws Exception {    
    	CommandOptions o = new CommandOptions(args);
    	Tap pipeline = new Tap(o);
        pipeline.createPhase()
            .reads(o.input)
            .groupBy("word")
            .writes(o.output);
        return pipeline.make();
    }
}
