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
package tap.formats.avro;

import java.util.Arrays;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.google.protobuf.GeneratedMessage;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class AvroFormat extends FileFormat {

	public static final byte FILE_SIGNATURE[] = {0x4F,0x62,0x6A,0x01};
		
	public void setupOutput(JobConf conf, Class<?> ignore) {
		conf.setOutputFormat(AvroOutputFormat.class);
		conf.setOutputKeyClass(AvroWrapper.class);
	}

	public void setupInput(JobConf conf, Class<?> ignore) {
		conf.setInputFormat(AvroInputFormat.class);
		conf.set(AvroJob.INPUT_IS_REFLECT, "true");
	}

	public String fileExtension() {
		return ".avro";
	}

	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.AVRO_FORMAT);
	}
	
	
	/**
	 * Compare file signature of Avro type file (uncompressed)
	 */
	@Override
	public boolean signature(byte[] header) {
		return Arrays.equals(FILE_SIGNATURE, Arrays.copyOfRange(header, 0, FILE_SIGNATURE.length));
	}

    @Override
    public boolean isCompatible(InputFormat format) {
        // TODO Auto-generated method stub
        return (format instanceof AvroInputFormat);
    }

    @Override
    public boolean instanceOfCheck(Object o) {
        // TODO Find better way than listing all of the other types
        return !(o instanceof String || o instanceof Text || o instanceof GeneratedMessage);
    }

}
