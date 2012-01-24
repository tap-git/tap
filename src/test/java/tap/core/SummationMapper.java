package tap.core;

// no-op mapper
public class SummationMapper extends TapMapper<CountRec, CountRec> {
    @Override
    public void map(CountRec in, CountRec out, TapContext<CountRec> context) {
        out.count = in.count;
        out.word = "sum";
        context.write((CountRec)out);
    }
}