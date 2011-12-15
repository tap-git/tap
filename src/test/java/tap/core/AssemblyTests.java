/*
 * Licensed to Think Big Analytics, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Think Big Analytics, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Copyright 2011 Think Big Analytics. All Rights Reserved.
 */
package tap.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Test;

import tap.formats.Formats;

public class AssemblyTests {

    @Test
    public void wordCount() {

        /* Set up a basic pipeline of map reduce */
        Tap wordcount = new Tap(getClass()).named("wordcount");
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
        Assert.assertNotNull("path", o.output);
        Pipe counts = Pipe.of(CountRec.class).at(o.output); // .avroFormat();

        Assert.assertEquals(Formats.AVRO_FORMAT, counts.getFormat());
        Assert.assertNotNull(counts.getPath());

        wordcount.produces(counts);

        Phase count = new Phase().reads(input).writes(counts).map(Mapper.class)
                .groupBy("word").reduce(Reducer.class);

        if (o.forceRebuild)
            wordcount.forceRebuild();

        wordcount.dryRun();

        wordcount.execute();
        File standard = new File("share/wordcount.out.avro");
        Assert.assertTrue("Can read standard", standard.canRead());
        Assert.assertTrue(standard.isFile());

        File file = new File("/tmp/out/part-00000.avro");
        Assert.assertTrue("Can read output", file.canRead());
        Assert.assertTrue(file.isFile());
        Assert.assertEquals("compare file to standard", standard.length(),
                file.length());
    }

    @Test
    public void summation() {
        /* Set up a basic pipeline of map reduce */
        Tap summation = new Tap(getClass()).named("summation");
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

        Pipe<CountRec> input = Pipe.of(CountRec.class).at(o.input);
        Pipe<OutputLog> output = Pipe.of(OutputLog.class).at(o.output);

        summation.produces(output);

        Phase sum = new Phase().reads(input).writes(output)
                .map(SummationMapper.class).groupBy("word")
                .reduce(SummationReducer.class);

        if (o.forceRebuild)
            summation.forceRebuild();

        // summation.dryRun();

        summation.execute();
    }

    public static class Mapper extends BaseMapper<String, CountRec> {
        @Override
        public void map(String line, CountRec out, TapContext<CountRec> context) {
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                out.word = tokenizer.nextToken();
                out.count = 1;
                context.write(out);
            }
        }
    }

    public static class Reducer extends BaseReducer<CountRec, CountRec> {

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