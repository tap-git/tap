package tap.sample;

import tap.core.Pipe;
import tap.core.TapReducer;

/**
 * Remove Bots from log file
 *
 */
public class BotFilter extends TapReducer<LogRec,LogRec> {

    @Override
    public void reduce(Pipe<LogRec> in, Pipe<LogRec> out) {
        for (LogRec r : in) {
            out.put(r);
        }
    }
    
}
