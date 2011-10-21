package tap.formats;

import tap.formats.avro.AvroFormat;
import tap.formats.json.JsonFormat;
import tap.formats.record.RecordFormat;
import tap.formats.unknown.UnknownFormat;
import tap.formats.text.TextFormat;

public enum Formats {
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