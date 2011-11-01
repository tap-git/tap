package tap.core;

// no-op mapper
public class SummationPipeMapper extends BaseMapper<CountRec, CountRec> {
    @Override
    public void map(CountRec in, Pipe<CountRec> out) {
        in.word = "sum";
        out.put(in);
    }
}