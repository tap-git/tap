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

import java.util.StringTokenizer;

import junit.framework.Assert;

import org.junit.Test;

public class TapTests {
	
	@Test
	public void testOptions() {
		String args[] = { "TapTests.tap", "-i", "/tmp/TapTests/maugham.txt", "-o",
				"/tmp/testOptions", "--force" };
		CommandOptions o = new CommandOptions(args);
		Tap t = new Tap(o);
	
		Assert.assertEquals("/tmp/TapTests/maugham.txt", o.input);
		Assert.assertEquals("/tmp/testOptions", o.output);
		Assert.assertEquals(true, o.forceRebuild);
		
	}

	/**
	 * @param args
	 */
	private void buildPipeline1(String[] args) {
		CommandOptions o = new CommandOptions(args);
		Tap pipeline = new Tap(o);

		pipeline.createPhase().reads(o.input).map(TapTests.Mapper.class)
				.combine(TapTests.Reducer.class).reduce(TapTests.Reducer.class).writes(o.output).sortBy("word");
		pipeline.named(args[0]).make();
	}

	@Test
	public void tap() {
		String args[] = { "TapTests.tap", "-i", "/tmp/TapTests/maugham.txt", "-o",
				"/tmp/TapTestsOutput", "--force" };

		buildPipeline1(args);
	}

	@Test
	public void tapNoForce() {
		String args[] = { "TapTests.tapNoForce", "-i", "/tmp/TapTests/maugham.txt", "-o",
				"/tmp/TapTestsOutput.tapNoForce",
				};

		buildPipeline1(args);
	}
	
	@Test
	public void tapMultiInput() {
		String args[] = { "TapTests.tapMultiInput", "-i", "/tmp/TapTests/*.txt", "-o",
				"/tmp/TapTestsOutput.tapMultiInput",
				"--force"};

		buildPipeline1(args);
	}

	@Test
	public void tapDirectoryInput() {
		String args[] = { "tapDirectoryInput", "-i", "/tmp/TapTests/", "-o",
				"/tmp/TapTestsOutput.tapDirectoryInput",
				"--force"};

		buildPipeline1(args);
	}

	public static class Mapper extends TapMapper<String, CountRec> {
		CountRec outrec = new CountRec();

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

	public static class Reducer extends TapReducer<CountRec, CountRec> {
		CountRec outrec = new CountRec();

		@Override
		public void reduce(Pipe<CountRec> in, Pipe<CountRec> out) {
			outrec.count = 0;
			for (CountRec rec : in) {
				outrec.word = rec.word;
				outrec.count += rec.count;
			}
			out.put(outrec);
		}
	}
}