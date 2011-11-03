package tap.core.compression;

import tap.core.compression.gz.GzipCompression;

public enum Compressions {
	GZIP_COMPRESSION {
		@Override
		public FileCompression getCompression() {
			return new GzipCompression();
		}

		@Override
		public String fileExtension() {
			return ".gz";
		}
	};

	/*
	 * (non-Javadoc)
	 * 
	 * @see tap.core.FileFormat#setPipeFormat(tap.core.Pipe)
	 */
	public abstract FileCompression getCompression();
	
	public abstract String fileExtension();

}