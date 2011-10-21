package tap.formats.unknown;

import org.apache.hadoop.mapred.JobConf;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class UnknownFormat extends FileFormat {

	public void setupOutput(JobConf conf) {
	}

	public void setupInput(JobConf conf) {    
    }

	public String fileExtension() {
		return ".unknown";
	}
	
	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.UNKNOWN_FORMAT);
	}

}
