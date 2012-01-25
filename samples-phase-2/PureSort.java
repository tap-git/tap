package tap.sample;
import tap.*;
public class PureSort extends Tap {
    @Override
    public int run(CommandOptions o) throws Exception {        
        createPhase()
            .reads(o.input).
            .groupBy("word")
            .writes(o.output);
        return make();
    }
}