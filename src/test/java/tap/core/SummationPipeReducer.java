package tap.core;


/*
 * Add up all of the word counts to produce a sum of all of the word counts
 */
public class SummationPipeReducer extends
		BaseReducer<CountRec, OutputLog> {

    private OutputLog outLog = new OutputLog();
	@Override
	public void reduce(Pipe<CountRec> in, Pipe<OutputLog> out) {
	    
	    outLog.description = "sum of words";
	    outLog.count = 0;
	    while (in.hasNext()) {
	        outLog.count += ((CountRec)in.next()).count;
	    }
		out.put(outLog);
	}
}