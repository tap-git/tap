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
package tap.formats.record;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import tap.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class RecordFormat extends FileFormat {

	@Override
	public void setupOutput(JobConf conf, Class<?> ignore) {
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(String.class);
	}

	@Override
	public void setupInput(JobConf conf, Class<?> ignore) {
		conf.setInputFormat(TextInputFormat.class);  
	}

	@Override
	public String fileExtension() {
		// TODO Auto-generated method stub
		return ".tsv";
	}

	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.RECORD_FORMAT);
	}

    @Override
    public boolean isCompatible(InputFormat format) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean instanceOfCheck(Object o) {
        // TODO Auto-generated method stub
        return false;
    }

}
