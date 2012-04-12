package tap.sample;



import tap.CommandOptions;
import tap.Pipe;
import tap.Tap;
import tap.TapMapper;
import tap.TapReducer;
import tap.formats.tapproto.Testmsg;
/*
 * usage: -i share/securities_data.tapproto -o /tmp/out -f
 */
public class TransactionSummary {
	 public static void main(String[] args) throws Exception {
    	 CommandOptions o = new CommandOptions(args);
    	 
        /* Set up a basic pipeline of map reduce */
        Tap t = new Tap(o).named("TransactionSummary");
        /* Parse options - just use the standard options - input and output location, time window, etc. */
       
       t.getConf().setJarByClass(TransactionSummary.class);
        if (o.input == null) {
            System.err.println("Must specify input directory");
            return;
        }
        if (o.output == null) {
            System.err.println("Must specify output directory");
            return;
        }

        t.createPhase().reads(o.input).map(Mapper.class).reduce(Reducer.class).groupBy("id").writes(o.output);
        t.make();
      

    }
	 
	 
	public static class Mapper extends TapMapper<Testmsg.SecuritiesRecord, Testmsg.SecuritiesRecord> {
		
		public void map(Testmsg.SecuritiesRecord msg, Pipe<Testmsg.SecuritiesRecord> out)
		{
			out.put(msg);
		}
	}
	
	public static class Reducer extends TapReducer<Testmsg.SecuritiesRecord, Testmsg.SecuritiesRecordSummary> {
		
		Testmsg.SecuritiesRecordSummary summary;
		public void reduce(Pipe<Testmsg.SecuritiesRecord> in, Pipe<Testmsg.SecuritiesRecordSummary> out)
		{
			int i = 0;
			int value = 0;
			String desc = null;
			long id = 0;
			for(Testmsg.SecuritiesRecord rec : in)
			{
				i++;
				value += rec.getStrike();
				desc = rec.getDesc();
				id = rec.getId();
	
			}
			
			summary = Testmsg.SecuritiesRecordSummary.newBuilder().setDesc(desc).
					setId(id).setNumTransactions(i).setTotalValue(value).build();
			
			out.put(summary);
			System.out.println(summary.toString());
			
		}
	}
	
}
