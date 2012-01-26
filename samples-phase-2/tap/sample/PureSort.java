package tap.sample;

import tap.*;
public class PureSort {
    public static void main(String[] args) throws Exception {    
    	CommandOptions o = new CommandOptions(args);
    	Tap tap = new Tap(o);
        tap.createPhase()
            .reads(o.input)
            .groupBy("word")
            .writes(o.output);
        tap.make(); ack
    }
}
