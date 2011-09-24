package tap.sample;

import org.apache.hadoop.conf.Configuration;

import tap.core.BaseMapper;
import tap.core.TapContext;

public class LogClean extends BaseMapper<LogRec,LogRec> {

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf != null) {
            //use conf.get
        }
    }
    
    @Override
    public void map(LogRec in, LogRec out, TapContext<LogRec> context) {
        if (!isDirty(in)) {
            context.write(in);
        }
    }

    private boolean isDirty(LogRec in) {
        return false;
    }


}
