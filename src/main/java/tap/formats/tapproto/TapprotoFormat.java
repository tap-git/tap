package tap.formats.tapproto;

import java.util.Arrays;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

import tap.core.Pipe;
import tap.core.mapreduce.input.TapfileInputFormat;
import tap.core.mapreduce.output.TapfileOutputFormat;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class TapprotoFormat extends FileFormat {

	private static final byte FILE_SIGNATURE[] = "tapproto".getBytes();
		
	public void setupOutput(JobConf conf, Class<?> protoClass) {
	    setupOutputImpl(conf, protoClass);
	}

	public void setupInput(JobConf conf, Class<?> protoClass) {
	    setupInputImpl(conf, protoClass);
	}

    private <M extends Message> void setupOutputImpl(JobConf conf, Class<?> protoClass) {
        conf.setOutputFormat((Class<? extends OutputFormat>)
            TapfileOutputFormat.getOutputFormatClass((Class<M>) protoClass, conf));
    }

    private <M extends Message> void setupInputImpl(JobConf conf, Class<?> protoClass) {
        conf.setInputFormat((Class<? extends InputFormat>)
            TapfileInputFormat.getInputFormatClass((Class<M>) protoClass, conf));
    }
	
	public String fileExtension() {
		return TapfileOutputFormat.EXT;
	}

	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.TAPPROTO_FORMAT);
	}
	
	/**
	 * Compare file signature of Tapproto type file
	 */
	@Override
	public boolean signature(byte[] header) {
		return Arrays.equals(FILE_SIGNATURE, Arrays.copyOfRange(header, 0, FILE_SIGNATURE.length));
	}

    @Override
    public boolean isCompatible(InputFormat format) {
        return (format instanceof TapfileInputFormat);
    }

    @Override
    public boolean instanceOfCheck(Object o) {
        return  o instanceof GeneratedMessage;
    }

}
