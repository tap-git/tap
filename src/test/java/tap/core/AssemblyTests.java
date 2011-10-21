package tap.core;

import static org.junit.Assert.*;

import java.io.File;
import java.util.StringTokenizer;

import junit.framework.Assert;
import tap.core.*;

import org.junit.Test;

public class AssemblyTests {
	public String word;
	public int count;

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

		String args[] = { "-i", "share/decameron.txt", "-o", "/tmp/out",
				"-f" };
		Assert.assertEquals(5, args.length);
		BaseOptions o = new BaseOptions();
		int result = o.parse(wordcount, args);
		Assert.assertEquals(0, result);
		Assert.assertNotNull("must specify input directory", o.input);
		Assert.assertNotNull("must specify output directory", o.output);

		Pipe input = new Pipe(o.input).stringFormat();
		Pipe<AssemblyTests> counts = new Pipe<AssemblyTests>(o.output);
		counts.setPrototype(this);

		wordcount.produces(counts);

		Phase count = new Phase().reads(input).writes(counts).map(Mapper.class)
				.groupBy("word").reduce(Reducer.class);

		if (o.forceRebuild)
			wordcount.forceRebuild();

		wordcount.dryRun();

		wordcount.execute();
	}

	public static class Mapper extends BaseMapper<String, AssemblyTests> {
		@Override
		public void map(String line, AssemblyTests out,
				TapContext<AssemblyTests> context) {
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				out.word = tokenizer.nextToken();
				out.count = 1;
				context.write(out);
			}
		}
	}

	public static class Reducer extends
			BaseReducer<AssemblyTests, AssemblyTests> {

		@Override
		public void reduce(Iterable<AssemblyTests> in, AssemblyTests out,
				TapContext<AssemblyTests> context) {
			out.count = 0;
			for (AssemblyTests rec : in) {
				out.word = rec.word;
				out.count += rec.count;
			}
			context.write(out);
		}

	}
}