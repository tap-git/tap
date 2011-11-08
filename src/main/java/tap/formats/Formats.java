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
package tap.formats;

import tap.formats.avro.AvroFormat;
import tap.formats.json.JsonFormat;
import tap.formats.record.RecordFormat;
import tap.formats.unknown.UnknownFormat;
import tap.formats.tapproto.TapprotoFormat;
import tap.formats.text.TextFormat;

public enum Formats {
	TAPPROTO_FORMAT {
		@Override
		public FileFormat getFileFormat() {
			return new TapprotoFormat();
		}
	},
	AVRO_FORMAT {
		@Override
		public FileFormat getFileFormat() {
			return new AvroFormat();
		}
	},
	JSON_FORMAT {
		@Override
		public FileFormat getFileFormat() {
			return new JsonFormat();
		}

	},
	RECORD_FORMAT {
		@Override
		public FileFormat getFileFormat() {
			return new RecordFormat();
		}
	},
	STRING_FORMAT {
		@Override
		public FileFormat getFileFormat() {
			return new TextFormat();
		}
	},
	UNKNOWN_FORMAT {
		@Override
		public FileFormat getFileFormat() {
			return new UnknownFormat();
		}
	};

	/*
	 * (non-Javadoc)
	 * 
	 * @see tap.core.FileFormat#setPipeFormat(tap.core.Pipe)
	 */
	public abstract FileFormat getFileFormat();

}