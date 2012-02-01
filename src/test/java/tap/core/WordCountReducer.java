package tap.core;

public class WordCountReducer extends TapReducer<CountRec, CountRec> {
	CountRec outrec = new CountRec();

	@Override
	public void reduce(Pipe<CountRec> in, Pipe<CountRec> out) {
		outrec.count = 0;
		for (CountRec rec : in) {
			outrec.word = rec.word;
			outrec.count += rec.count;
		}
		out.put(outrec);
	}
}