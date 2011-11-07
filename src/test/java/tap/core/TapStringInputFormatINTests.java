package tap.core;

import static org.junit.Assert.*;

import java.io.File;

import junit.framework.Assert;

import org.junit.Test;

import tap.core.AssemblyTests.Mapper;
import tap.core.AssemblyTests.Reducer;
import tap.formats.Formats;

public class TapStringInputFormatInTests {

    @Test
    public void wordCountIntuitStringIN() {

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

//        Pipe input = new Pipe(o.input).stringFormat();
        Pipe input = new Pipe(o.input);
        Assert.assertEquals(Formats.STRING_FORMAT,input.getFormat());
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

}
