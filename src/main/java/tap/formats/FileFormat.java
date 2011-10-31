package tap.formats;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import tap.core.Pipe;

@SuppressWarnings("deprecation")
public abstract class FileFormat {

	public abstract void setupOutput(JobConf conf, Class<?> protoClass);

	public abstract void setupInput(JobConf conf, Class<?> protoClass);

	/**
	 * Obtain default file extension for the FileFormat
	 * @return The file extension.
	 */
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

	/**
	 * Is this File format compatible with the Hadoop input type (class)
	 * @param foramt The format as determined by Hadoop / Tap
	 * @return true if compatible.
	 */
	public abstract boolean isCompatible(InputFormat foramt);

}