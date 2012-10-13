package tap.sample;


import tap.CommandOptions;
import tap.Pipe;
import tap.Tap;
import tap.TapMapper;
import tap.TapReducer;
import tap.core.*;


//groupBy indicates the grouping for presentation to reducer.  sortby indicates how records should be sorted, i.e., 
//which fields are part of the key.  if groupBy is not set to anything then by default the grouping at the 'key' level.
//more logically we should require the groupBy be specified and change sort to subsort.
public class SortTestAvroInput {

    public static void main(String[] args) throws Exception {
    	String a[] = { "-o", "/tmp/sorttest", "-i", "share/test_data.avro", "-f" };
    	CommandOptions o = new CommandOptions(a);
        /* Set up a basic pipeline of map reduce */
        Tap wordcount = new Tap(o).named("wordcount");
        /* Parse options - just use the standard options - input and output location, time window, etc. */
        
        if (o.input == null) {
            System.err.println("Must specify input directory");
            return;
        }
        if (o.output == null) {
            System.err.println("Must specify output directory");
            return;
        }

        wordcount.createPhase().reads(o.input).writes(o.output).map(Mapper.class).groupBy("group").sortBy("extra, subsort").reduce(Reducer.class);
        
        wordcount.make();
    }

    public static class key {
        public String group;
        public String extra;
        public String subsort;
    }
    

    public static class Mapper extends TapMapper<key,key> {
    	
    	private key outrec = new key();
        @Override
        public void map(key in, Pipe<key> out) {
        	
        	
            out.put(in);
        }        
    }

    public static class Reducer  extends TapReducer<key,key> {
    	private key outrec = new key();

        @Override
        public void reduce(Pipe<key> in, Pipe<key> out) {
        	System.out.println("**************");
            for (key rec : in) {
            	
            	outrec.group = rec.group;
            	outrec.extra = rec.extra;
            	outrec.subsort = rec.subsort;
            	System.out.println("REDUCER::" + outrec.group + " " + outrec.extra + " " + outrec.subsort);
                out.put(outrec);
            }
          
        }
        
    }
  
}
