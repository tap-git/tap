package thinkbig;


import java.util.StringTokenizer;

import tap.CommandOptions;
import tap.Pipe;
import tap.Tap;
import tap.TapMapper;
import tap.TapReducer;
import tap.sample.WordCount;

import thinkbig.examples.messages.*;


public class WordCount {

	public static void main(String[] args) throws Exception {
		
		
		 CommandOptions o = new CommandOptions(args);
    	 
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

	        wordcount.getConf().setJarByClass(WordCount.class);
	        
	        wordcount.createPhase().reads(o.input).writes(o.output).map(Mapper.class).
	            groupBy("word").reduce(Reducer.class);

	        wordcount.make();
	    }

	   
	    

	    public static class Mapper extends TapMapper<String,Wordcountmsg.WordCountRecord> {
	    	private Wordcountmsg.WordCountRecord outrec;
	    	
	        @Override
	        public void map(String line, Pipe<Wordcountmsg.WordCountRecord> out) {
	            StringTokenizer tokenizer = new StringTokenizer(line);
	            while (tokenizer.hasMoreTokens()) {
	            	outrec = Wordcountmsg.WordCountRecord.newBuilder().setWord(tokenizer.nextToken()).setCount(1).build();
	                out.put(outrec);
	            }
	        }        
	    }

	    public static class Reducer extends TapReducer<Wordcountmsg.WordCountRecord,Wordcountmsg.WordCountRecord> {
	    	
	    	private Wordcountmsg.WordCountRecord outrec;
	    	
	        @Override
	        public void reduce(Pipe<Wordcountmsg.WordCountRecord> in, Pipe<Wordcountmsg.WordCountRecord> out) {
	            int count = 0;
	            String word = null;
	            for (Wordcountmsg.WordCountRecord rec : in) {
	            	word = rec.getWord();
	            	count++;
	            }
	            outrec = Wordcountmsg.WordCountRecord.newBuilder().setWord(word).setCount(count).build();
	            out.put(outrec);
	            System.out.println(outrec.toString());
	        }
	        
	    }
	}


