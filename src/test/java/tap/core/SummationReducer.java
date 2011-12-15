package tap.core;


/*
 * Add up all of the word counts to produce a sum of all of the word counts
 */
public class SummationReducer extends
		BaseReducer<CountRec, OutputLog> {

	@Override
	public void reduce(Iterable<CountRec> in, OutputLog out,
			TapContext<OutputLog> context) {
		out.description = "sum of words";
		out.count = 0;
		for (CountRec rec : in) {
			out.count += rec.count;
		}
		context.write(out);
	}

}