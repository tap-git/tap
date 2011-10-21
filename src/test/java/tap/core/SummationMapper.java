package tap.core;

// no-op mapper
public class SummationMapper extends BaseMapper<AssemblyTests, AssemblyTests> {
    public void map(AssemblyTests in, AssemblyTests out, TapContext<AssemblyTests> context) {
        context.write((AssemblyTests)in);
    }
}