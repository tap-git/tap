package tap.core;

import tap.CountRec;
import tap.OutputLog;
import tap.Pipe;
import tap.TapReducer;


/*
 * Add up all of the word counts to produce a sum of all of the word counts
 */
public class SummationReducer extends
		TapReducer<CountRec, OutputLog> {
	private OutputLog outrec = new OutputLog();
	@Override
	public void reduce(Pipe<CountRec> in, Pipe<OutputLog> out) {
		outrec.description = "sum of words";
		outrec.count = 0;
		for (CountRec rec : in) {
			outrec.count += rec.count;
		}
		out.put(outrec);
	}

}