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

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

import tap.core.Pipe;
import tap.formats.Formats;
import tap.formats.tapproto.Testmsg;
import tap.formats.tapproto.Testmsg.TestMsg;

public class TapprotoPipeTests {

    @Test
    public void testMessageParentage() {
        TestMsg m = Testmsg.TestMsg.getDefaultInstance();
        Assert.assertNotNull(m);
        Assert.assertTrue("is a protobuf", m instanceof com.google.protobuf.GeneratedMessage);
        Pipe<TestMsg> pipe = Pipe.of(TestMsg.class);
        Assert.assertTrue("pipe type", pipe.getPrototype() instanceof TestMsg);
        Assert.assertTrue("pipe type", pipe.getPrototype() instanceof com.google.protobuf.GeneratedMessage);
        Assert.assertEquals(Formats.TAPPROTO_FORMAT, pipe.getFormat());
    }

}