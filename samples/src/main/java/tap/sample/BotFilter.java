package tap.sample;

import tap.core.BaseReducer;
import tap.core.TapContext;

public class BotFilter extends BaseReducer<LogRec,LogRec> {

    @Override
    public void reduce(Iterable<LogRec> in, LogRec out, TapContext<LogRec> context) {
        for (LogRec r : in) {
            context.write(r);
        }
    }
    
}
