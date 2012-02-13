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
 * Copyright 2010 Think Big Analytics. All Rights Reserved.
 */
package tap.core;


import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;

import tap.Phase;
import tap.core.io.BinaryKey;
import tap.core.io.Bytes;

/** The {@link RawComparator} used by jobs configured with {@link AvroJob}. */
public class BinaryKeyComparator extends Configured implements RawComparator<AvroWrapper<BinaryKey>> {

    private Schema schema;

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return compareBinary(b1, s1, l1, b2, s2, l2);
    }
    
    public int compare(AvroWrapper<BinaryKey> x, AvroWrapper<BinaryKey> y) {
    	BinaryKey k1 = x.datum(), k2 = y.datum();
    	return compareBinary(
    			k1.getBuffer(), 0, k1.getLength(),
    			k2.getBuffer(), 0, k2.getLength());
    }
    
    private int compareBinary(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    	s1 += BinaryKey.KEY_BYTES_OFFSET;
    	s2 += BinaryKey.KEY_BYTES_OFFSET;
    	l1 -= BinaryKey.KEY_BYTES_OFFSET;
    	l2 -= BinaryKey.KEY_BYTES_OFFSET;
    	
        return Bytes.compare(b1, s1, l1, b2, s2, l2);
    }
}
