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
import java.io.IOException;
import junit.framework.Assert;

import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;

public class TapTests {

	@Test
	public void testOptions() {
		String args[] = { "TapTests.tap", "-i", "shared/decameron.txt", "-o",
				"/tmp/testOptions", "--force" };
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);
		tap.alerter(new TapUnitTestAlerter());
	
		Assert.assertEquals("shared/decameron.txt", o.input);
		Assert.assertEquals("/tmp/testOptions", o.output);
		Assert.assertEquals(true, o.forceRebuild);	
	}
	
	@Test
	public void tapDirectoryInput() {
		String args[] = { "tapDirectoryInput", "-i", "share/multi", "-o",
				"/tmp/TapTestsOutput.tapDirectoryInput",
				"--force"};
	
		buildPipeline1(args);
	}

	/**
	 * @param args
	 */
	private void buildPipeline1(String[] args) {
		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);
		tap.alerter(new TapUnitTestAlerter());
		
		tap.createPhase()
				.map(WordCountMapper.class)
				.combine(WordCountReducer.class)
				.reduce(WordCountReducer.class).sortBy("word");
		tap.getConf().setInt("io.sort.mb", 10); //override default of 100mb
		tap.make();
	}

	@Test
	public void tap() {
		String args[] = { "TapTests.tap", "-i", "share/decameron.txt", "-o",
				"/tmp/TapTestsOutput", "--force" };

		buildPipeline1(args);
		File f = new File(args[4]+"/part-00000.avro");
		Assert.assertTrue("File exists", f.exists());
	}

	@Test
	public void tapNoForce() {
		String args[] = { "TapTests.tapNoForce", "-i", "share/decameron.txt", "-o",
				"/tmp/TapTestsOutput.tapNoForce",
				};

		buildPipeline1(args);
	}
	
	@Test
	public void tapMultiInput() throws IOException {
		String args[] = { "TapTests.tapMultiInput", "-i", "share/multi/20120203-132830417*.txt", "-o",
				"/tmp/TapTestsOutput.tapMultiInput",
				"--force"};

		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);
		Phase phase1 = tap.createPhase().reads(o.input)
				.map(WordCountMapper.class).combine(WordCountReducer.class)
				.reduce(WordCountReducer.class).sortBy("word").writes(o.output);
		tap.getConf().setInt("io.sort.mb", 10); //override default of 100mb
		//tap.make();
		Assert.assertNotNull(tap.getConf());
		DFSStat stat = new DFSStat(o.input, tap.getConf());
		if (null != stat.getStatuses())
		for(FileStatus s: stat.getStatuses()) {
			System.out.println(s.getPath());
		}
		
	}
}