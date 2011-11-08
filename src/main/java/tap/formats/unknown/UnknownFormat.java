package tap.formats.unknown;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class UnknownFormat extends FileFormat {

	public void setupOutput(JobConf conf, Class<?> ignore) {
	}

	public void setupInput(JobConf conf, Class<?> ignore) {    
    }

	public String fileExtension() {
		return ".unknown";
	}
	
	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.UNKNOWN_FORMAT);
	}

    @Override
    public boolean isCompatible(InputFormat format) {
        return false;
    }

    @Override
    public boolean instanceOfCheck(Object o) {
        // TODO Auto-generated method stub
        return false;
    }

}
