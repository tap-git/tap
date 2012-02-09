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
package tap;

import static org.junit.Assert.*;
import junit.framework.Assert;

import java.lang.reflect.ParameterizedType;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import tap.Pipe;
import tap.formats.*;
import tap.util.ObjectFactory;

/*
 * @Author Douglas Moore
 * Test file path format detection
 */
public class PipeTests {

    public class CountRec {
        String word;
        int count;
    }

    @Test
    public void testInputDirectory() {
        Pipe<Text> input = new Pipe<Text>("share/multi/01");
        input.setConf(new JobConf());
        Assert.assertTrue(input.isValidInput());
    }
    
    @Test
    public void testInputDirectory2() {
        Pipe<Text> input = new Pipe<Text>("share/multi/01/");
        input.setConf(new JobConf());
        Assert.assertTrue(input.isValidInput());
    }
    
    // Not implementing this requirement now
    //@Test
    public void testInputDirectoryRecursive() {
        Pipe<Text> input = new Pipe<Text>("share/multi");
        input.setConf(new JobConf());
        Assert.assertTrue(!input.isValidInput());
    }
    
    @Test
    public void testInputWildcard() {
        Pipe<Text> input = new Pipe<Text>("share/multi/01/*.txt");
        input.setConf(new JobConf());
        Assert.assertTrue(input.isValidInput());
    }
    
    @Test
    public void testInputSingle() {
        Pipe<OutputLog> input = new Pipe<OutputLog>("share/decameron.txt");
        input.setConf(new JobConf());
        Assert.assertTrue(input.isValidInput());
    }
    
    @Test
    public void testInputNonExistent() {
        Pipe<OutputLog> input = new Pipe<OutputLog>("share/missing.txt");
        input.setConf(new JobConf());
        Assert.assertFalse(input.isValidInput());
    }
    
    
    @Test
    public void testPipeAvroDefault() {
        Pipe<OutputLog> input = new Pipe<OutputLog>("share/");
        input.setConf(new JobConf());
        Assert.assertEquals(Formats.UNKNOWN_FORMAT, input.getFormat());
    }

    @Test
    public void testPipeOfAvroDefault() {
        Pipe<OutputLog> input = Pipe.of(OutputLog.class).at("/tmp/out");
        Assert.assertEquals(Formats.AVRO_FORMAT, input.getFormat());
    }

    @Test
    public void prototype() {
        Pipe pipe = new Pipe(CountRec.class);
        Assert.assertNotNull(pipe);
        Assert.assertNotNull(pipe.getPrototype());
        System.out.println("pipe prototype " + pipe.getPrototype());
        Assert.assertEquals(pipe.getPrototype().toString(), CountRec.class, pipe.getPrototype());
    }
    
    @Test
    public void prototypeStringTest() {
    	Object o = new String("");
    	Assert.assertTrue("String ", o instanceof String);
    	Object o2 = new MyPipeTestRecord();
    	Assert.assertTrue("My PipeTestRecord", o2 instanceof MyPipeTestRecord);
    }
    
    class MyPipeTestRecord {
    	int id;
    	String name;
    }
    
    @Test
    public void prototypeSetString() {
    	Pipe pipe = new Pipe("share/decameron.txt");
    	pipe.setPrototype(new String());
    	Assert.assertNotNull(pipe.getPrototype());
    	Assert.assertEquals("Pipe Prototype for set new String() " + 
    			pipe.getPrototype().toString(),
    			java.lang.String.class, 
    			pipe.getPrototype().getClass());
    }
    
    @Test
    public void prototypeSetStringClass() {
    	Pipe pipe = new Pipe("share/decameron.txt");
    	pipe.setPrototype(new String());
    	Assert.assertNotNull(pipe.getPrototype());
    	Assert.assertEquals(pipe.getPrototype().toString(),
    			java.lang.String.class,
    			pipe.getPrototype().getClass());
    }
    
    @Test
    public void prototypeSetMyPipeTestRecordClass() {
    	Pipe pipe = new Pipe("share/decameron.txt");
    	pipe.setPrototype(new MyPipeTestRecord());
    	Assert.assertNotNull(pipe.getPrototype());
    	Assert.assertEquals(pipe.getPrototype().toString(),
    			MyPipeTestRecord.class,
    			pipe.getPrototype().getClass());
    }
    
    @Test
    public void prototypeSetMyPipeTestRecord() {
    	Pipe pipe = new Pipe("share/decameron.txt");
    	pipe.setPrototype(new MyPipeTestRecord());
    	Assert.assertNotNull(pipe.getPrototype());
    	Assert.assertNotSame(pipe.getPrototype().toString(),
    			MyPipeTestRecord.class,
    			pipe.getPrototype());
    }
    
    @Test
    public void prototypeString() {
        Pipe pipe = new Pipe("share/decameron.txt");
        Assert.assertNotNull(pipe);
        Assert.assertNotNull(pipe.getFormat());
        System.out.println("pipe format " + pipe.getFormat().toString());
        Assert.assertNotSame(pipe.getFormat().toString(), Formats.JSON_FORMAT, pipe.getFormat());
        Assert.assertNull(pipe.getPrototype());
        System.out.println("pipe prototype " + pipe.getPrototype());
    }

    @Test
    public void stringFormat() {
        Pipe<CountRec> pipe = new Pipe<CountRec>("nonexistent.txt");
        assertEquals(Formats.STRING_FORMAT, pipe.getFormat());
    }

    @Test
    public void avroFormat() {
        Pipe<OutputLog> pipe = new Pipe<OutputLog>("nonexistent.avro");
        assertEquals(Formats.AVRO_FORMAT, pipe.getFormat());
    }

    @Test
    public void testFormat() {
        Pipe pipe = new Pipe("nonexistent.json");
        assertEquals(Formats.JSON_FORMAT, pipe.getFormat());
    }

    @Test
    public void testUnknownFormat() {
        Pipe pipe = new Pipe("nonexistent.unknown");
        assertEquals(Formats.UNKNOWN_FORMAT, pipe.getFormat());
    }

    @Test
    public void testTextGziped() {
        Pipe<String> pipe = new Pipe("nonexistent.txt.gz");
        assertEquals(Formats.STRING_FORMAT, pipe.getFormat());
        assertTrue(pipe.isCompressed());
    }

    @Test
    public void testEquals() {
        Pipe pipe1 = new Pipe("nonexistent.txt");
        Pipe pipe2 = new Pipe("nonexistent.txt");
        Pipe pipe3 = new Pipe<Text>("nonexistent.txt");
        assertTrue(pipe1.equals(pipe2));
        assertTrue(pipe1.equals(pipe3));
    }
    
    @Test
    public void pipeOftest() {
        Pipe pipe = Pipe.of(OutputLog.class);
        Assert.assertNotNull("pipe", pipe);
        Assert.assertTrue(pipe instanceof Pipe);
        Assert.assertTrue("pipe type", pipe.getPrototype() instanceof OutputLog);
        
        Pipe pipe2 = Pipe.of(new CountRec());
        Assert.assertNotNull("pipe", pipe2);
        Assert.assertTrue(pipe2 instanceof Pipe);
        Assert.assertTrue("pipe type", pipe2.getPrototype() instanceof CountRec);
    }
    
    @Test
    public void pipePathtest() {
        Pipe pipe = Pipe.of(OutputLog.class).at("/tmp/PhaseTestsOut4");
        Assert.assertEquals("path", "/tmp/PhaseTestsOut4", pipe.getPath());

        Pipe pipe2 = new Pipe("/tmp/xyz");
        Assert.assertEquals("path", "/tmp/xyz", pipe2.getPath());
     
    }
    
    @Test
    public void anonPipe() {
        Pipe pipe = new Pipe("/tmp/abc");
        Assert.assertEquals(Formats.UNKNOWN_FORMAT, pipe.getFormat());
    }
}