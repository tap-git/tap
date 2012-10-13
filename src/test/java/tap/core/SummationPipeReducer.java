package tap.core;

import tap.CountRec;
import tap.OutputLog;
import tap.Pipe;
import tap.TapReducer;


/*
 * Add up all of the word counts to produce a sum of all of the word counts
 */
public class SummationPipeReducer extends
		TapReducer<CountRec, OutputLog> {

    private OutputLog outLog = new OutputLog("sum of words", 0);
    
	@Override
	public void reduce(Pipe<CountRec> in, Pipe<OutputLog> out) {
	    int loopCount = 0;
	    CountRec val;
	    while (in.hasNext()) {
	        val = in.next();
	        //System.out.printf("<CountRec> (%s, %d) \n", val.word, val.count);
	        outLog.count += val.count;
	        loopCount ++;
	    }
	    System.out.printf("SumationPipeReducer: Loop Count=%d Outputing outlog.count = %d\n", loopCount, outLog.count);
		out.put(outLog);
	}
}