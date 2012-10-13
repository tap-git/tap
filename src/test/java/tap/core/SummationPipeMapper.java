package tap.core;

import tap.CountRec;
import tap.Pipe;
import tap.TapMapper;

// no-op mapper
public class SummationPipeMapper extends TapMapper<CountRec, CountRec> {
    @Override
    public void map(CountRec in, Pipe<CountRec> out) {
        in.word = "sum";
        out.put(in);
    }
}