package tap.core;

import java.util.StringTokenizer;

import tap.CountRec;
import tap.Pipe;
import tap.TapMapper;

public class WordCountMapper extends TapMapper<String, CountRec> {
	private CountRec outrec = new CountRec();

	@Override
	public void map(String line, Pipe<CountRec> out) {
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			outrec.word = tokenizer.nextToken();
			outrec.count = 1;
			out.put(outrec);
		}
	}
}
