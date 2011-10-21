package tap.core;


/*
 * Add up all of the word counts to produce a sum of all of the word counts
 */
public class SummationReducer extends
		BaseReducer<AssemblyTests, OutputLog> {

	@Override
	public void reduce(Iterable<AssemblyTests> in, OutputLog out,
			TapContext<OutputLog> context) {
		out.description = "sum of words";
		out.count = 0;
		for (AssemblyTests rec : in) {
			out.count += rec.count;
		}
		context.write(out);
	}

}