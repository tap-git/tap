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
package tap.formats.avro;

import java.util.ArrayList;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import tap.Phase;
import tap.core.ReflectionKeyExtractor;
import tap.core.io.BinaryKey;

public class BinaryKeyPartitioner<V> implements Partitioner<AvroKey<BinaryKey>,AvroValue<V>> {

    private ArrayList<String> groupNames;

    @Override
    public void configure(JobConf conf) {
        //Schema schema = Schema.parse(conf.get(ColPhase.MAP_OUT_VALUE_SCHEMA));
        String groupBy = conf.get(Phase.GROUP_BY);
        String[] groupFields = groupBy==null ? new String[0] : groupBy.split(",");
        groupNames = new ArrayList<String>(groupFields.length);
        
        ReflectionKeyExtractor.addFieldnames(groupNames, groupFields);
    }

    @Override
    public int getPartition(AvroKey<BinaryKey> key, AvroValue<V> value, int numPartitions) {
    	int hash = hashBytes(
    			key.datum().getBuffer(),
    			BinaryKey.KEY_BYTES_OFFSET, key.datum().keyBytesLength());
    	
    	return Math.abs(hash % numPartitions);
    }
    
    private static int hashBytes(byte[] bytes, int offset, int length) {
    	int end = offset + length;
    	int hash = 1;
    	for (int i = offset; i < end; i++)
    		hash = (31 * hash) + (int)bytes[i];
    	return hash;
    }
}

