package tap.compression;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;

import tap.core.Pipe;

@SuppressWarnings("deprecation")
public abstract class FileCompression {

	protected Class<? extends CompressionCodec> codec = null;
	
	protected FileCompression(Class<? extends CompressionCodec> codec) {
		this.codec = codec;
	}
	public abstract void setupOutput(JobConf conf);

	public abstract void setupInput(JobConf conf);

	// file extension
	public abstract String fileExtension();

	/*
	 * Does path name indicate a matching file name
	 */
	public boolean matches(String path) {
		return path.endsWith(fileExtension());
	}
	
	public boolean signature(byte[] header) {
		//TODO:  May need to handle auto inspection
		return false;
	}

	public abstract void setPipeCompression(Pipe pipe) ;
	
	public abstract Class<? extends CompressionCodec> getCodec();
}
