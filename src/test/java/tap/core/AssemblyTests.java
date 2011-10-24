package tap.core;

import java.io.File;
import java.util.StringTokenizer;

import junit.framework.Assert;
import org.junit.Test;

import tap.formats.Formats;

public class AssemblyTests {
	
	@Test
	public void wordCount() {

		/* Set up a basic pipeline of map reduce */
		Assembly wordcount = new Assembly(getClass()).named("wordcount");
		/*
		 * Parse options - just use the standard options - input and output
		 * location, time window, etc.
		 */
		File infile = new File("share/decameron.txt");
		System.out.println(infile.getAbsolutePath());

		String args[] = { "-i", "share/decameron.txt", "-o", "/tmp/out", "-f" };
		Assert.assertEquals(5, args.length);
		BaseOptions o = new BaseOptions();
		int result = o.parse(wordcount, args);
		Assert.assertEquals(0, result);
		Assert.assertNotNull("must specify input directory", o.input);
		Assert.assertNotNull("must specify output directory", o.output);

		Pipe input = new Pipe(o.input).stringFormat();
		Pipe<CountRec> counts = new Pipe<CountRec>(o.output);
		counts.setPrototype(new CountRec());

		wordcount.produces(counts);

		Phase count = new Phase().reads(input).writes(counts).map(Mapper.class)
				.groupBy("word").reduce(Reducer.class);

		if (o.forceRebuild)
			wordcount.forceRebuild();

		wordcount.dryRun();

		wordcount.execute();
	}

	// Job failing.....
	@Test
	public void summation() {
		/* Set up a basic pipeline of map reduce */
		Assembly summation = new Assembly(getClass())
				.named("summation");
		/*
		 * Parse options - just use the standard options - input and output
		 * location, time window, etc.
		 */

		String args[] = { "-o", "/tmp/wordcount", "-i", "/tmp/out", "-f" };
		Assert.assertEquals(5, args.length);
		BaseOptions o = new BaseOptions();
		int result = o.parse(summation, args);
		Assert.assertEquals(0, result);
		Assert.assertNotNull("must specify input directory", o.input);
		Assert.assertNotNull("must specify output directory", o.output);

		Pipe<CountRec> input = new Pipe<CountRec>(o.input);
		input.setPrototype(new CountRec());

		Pipe<OutputLog> output = new Pipe<OutputLog>(o.output);
		output.setPrototype(new OutputLog());

		summation.produces(output);

		Phase sum = new Phase().reads(input).writes(output)
				.map(SummationMapper.class).groupBy("count")
				.reduce(SummationReducer.class);

		if (o.forceRebuild)
			summation.forceRebuild();

		summation.dryRun();

		summation.execute();
	}

	public static class Mapper extends BaseMapper<String, CountRec> {
		@Override
		public void map(String line, CountRec out,
				TapContext<CountRec> context) {
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				out.word = tokenizer.nextToken();
				out.count = 1;
				context.write(out);
			}
		}
	}

	public static class Reducer extends
			BaseReducer<CountRec, CountRec> {

		@Override
		public void reduce(Iterable<CountRec> in, CountRec out,
				TapContext<CountRec> context) {
			out.count = 0;
			for (CountRec rec : in) {
				out.word = rec.word;
				out.count += rec.count;
			}
			context.write(out);
		}

	}
}