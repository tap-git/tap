package tap.formats.tapproto;

import java.util.Arrays;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.mapred.JobConf;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class TapprotoFormat extends FileFormat {

	private static final byte FILE_SIGNATURE[] = "tapproto".getBytes();
		
	public void setupOutput(JobConf conf) {
		// @TODO: protobuf
	}

	public void setupInput(JobConf conf) {
		// @TODO: protobuf
	}

	public String fileExtension() {
		return ".proto";
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

}
