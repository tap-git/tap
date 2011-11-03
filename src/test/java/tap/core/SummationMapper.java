package tap.core;

// no-op mapper
public class SummationMapper extends BaseMapper<CountRec, CountRec> {
    @Override
    public void map(CountRec in, CountRec out, TapContext<CountRec> context) {
        context.write((CountRec)in);
    }
}