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
package tap.formats.tapproto;

import java.util.List;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.junit.Test;

import tap.core.CommandOptions;
import tap.core.CountRec;
import tap.core.Phase;
import tap.core.PhaseError;
import tap.core.Pipe;
import tap.core.Tap;
import tap.core.TapMapper;
import tap.core.TapReducer;
import tap.core.WordCountMapper;
import tap.core.WordCountReducer;
import tap.formats.Formats;
import tap.formats.tapproto.Testmsg;
import tap.formats.tapproto.Testmsg.TestMsg;

public class TapprotoPipeTests {

    @Test
    public void testMessageParentage() {
        TestMsg m = Testmsg.TestMsg.getDefaultInstance();
        Assert.assertNotNull(m);
        Assert.assertTrue("is a protobuf", m instanceof com.google.protobuf.Message);
        Pipe<TestMsg> pipe = Pipe.of(TestMsg.class);
        Assert.assertTrue("pipe type", pipe.getPrototype() instanceof TestMsg);
        Assert.assertTrue("pipe type", pipe.getPrototype() instanceof com.google.protobuf.Message);
        Assert.assertEquals(Formats.TAPPROTO_FORMAT, pipe.getFormat());
    }
    
    @Test
    public void willSetProtoOutputType() {
		String args[] = { "BindingTests.mapOutTest", "-i", "share/decameron.txt", "-o",
				"/tmp/TapTestsOutput", "--force" };

		CommandOptions o = new CommandOptions(args);
		Tap tap = new Tap(o);

		Phase phase1 = tap.createPhase()
				.reads(o.input)
				.map(Mapper.class)
				.groupBy("data")
				.reduce(Reducer.class)
				.writes(o.output);
		tap.produces(phase1.output());
		List<PhaseError> phaseErrors = phase1.plan(tap);
		for(PhaseError e: phaseErrors) {
			System.out.println(e.getMessage());
		}
		Assert.assertEquals(0, phaseErrors.size());
		Assert.assertEquals("TAPPROTO_FORMAT", phase1.getOutputs().get(0).getFormat().toString());    	
    }
    
    public static class Mapper extends TapMapper<String, TestMsg> {
    	public void map(String line, Pipe<TestMsg> out) {
    	}
    }
    
    public static class Reducer extends TapReducer<TestMsg, TestMsg> {
    	CountRec outrec = new CountRec();

    	public void reduce(Pipe<TestMsg> in, Pipe<TestMsg> out) {
    	}
    }
}