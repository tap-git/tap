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
package tap.formats.text;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class TextFormat extends FileFormat {

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
		return ".txt";
	}

	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.STRING_FORMAT);
		pipe.setPrototype(new String(""));
	}
	
	@Override
	public boolean isCompatible(InputFormat format) {
	    return (format instanceof TextInputFormat);
	}

	/**
	 * Should contain one new line and range of characters are printable
	 */
	@Override
	public boolean signature(byte[] header) {

		int recordDeliminatorCount = 0;
		boolean hasPrintableOnly = false;
		
		if (header[0] == 0xFF) {
			// Unicode ?
		} else {
			for (byte b : header) {
				if (b == '\n') {
					recordDeliminatorCount++;
					continue;
				} else if (b >= 32 && b < 127 || b == '\r' || b == '\t') {
					if (recordDeliminatorCount >= 2) {
						// done after reaching two records worth of scanning
						break;
					}
					continue;
				}
				hasPrintableOnly = false;
				break;
			}
			hasPrintableOnly = true;
		}
		return (recordDeliminatorCount > 0) && hasPrintableOnly;
	}

    @Override
    public boolean instanceOfCheck(Object o) {
        return o instanceof String || o instanceof Text;
    }
}