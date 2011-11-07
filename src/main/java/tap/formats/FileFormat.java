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

	/**
	 * Does path name indicate a matching file name. A null path returns a false.
	 * @param path The path name of the file.
	 * @return true if the file name extension matches the FileFormat type.
	 */
	public boolean matches(String path) {
		return (null != path) && path.endsWith(fileExtension());
	}
	
	/**
	 * Perform multiple tests on Pipe against the FileFormat
	 * @param pipe to check
	 * @return true if a match
	 */
	public boolean matches(Pipe pipe) {
	    return (null != pipe.getPrototype()) && instanceOfCheck(pipe.getPrototype()) || matches(pipe.getUncompressedPath());
	}

	/**
	 * Check the instance type for a match
	 * @param o
	 * @return
	 */
	public abstract boolean instanceOfCheck(Object o);
	
	public boolean signature(byte[] header) {
		return false;
	}

	public abstract void setPipeFormat(Pipe pipe) ;

	/**
	 * Is this File format compatible with the Hadoop input type (class)
	 * @param format The format as determined by Hadoop / Tap
	 * @return true if compatible.
	 */
	public abstract boolean isCompatible(InputFormat format);

}