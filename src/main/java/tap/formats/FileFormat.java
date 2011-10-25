package tap.formats;

import org.apache.hadoop.mapred.JobConf;
import tap.core.Pipe;

@SuppressWarnings("deprecation")
public abstract class FileFormat {

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
		return false;
	}

	public abstract void setPipeFormat(Pipe pipe) ;

}