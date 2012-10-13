package tap.sample;

import org.apache.hadoop.conf.Configuration;

import tap.Pipe;
import tap.TapMapper;

public class LogClean extends TapMapper<LogRec,LogRec> {

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf != null) {
            //use conf.get
        }
    }
    
    @Override
    public void map(LogRec in, Pipe<LogRec> out) {
        if (!isDirty(in)) {
            out.put(in);
        }
    }

    private boolean isDirty(LogRec in) {
        return false;
    }


}
