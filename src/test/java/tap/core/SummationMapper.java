package tap.core;

import tap.CountRec;
import tap.Pipe;
import tap.TapMapper;

public class SummationMapper extends TapMapper<CountRec, CountRec> {
	private CountRec outrec = new CountRec();;
    @Override
    public void map(CountRec in, Pipe<CountRec> out) {
        outrec.count = in.count;
        outrec.word = "sum";
        out.put(outrec);
    }
}