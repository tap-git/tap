package tap.formats.avro;

import java.util.Arrays;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class AvroFormat extends FileFormat {

	public static final byte FILE_SIGNATURE[] = {0x4F,0x62,0x6A,0x01};
		
	public void setupOutput(JobConf conf, Class<?> ignore) {
		conf.setOutputFormat(AvroOutputFormat.class);
		conf.setOutputKeyClass(AvroWrapper.class);
	}

	public void setupInput(JobConf conf, Class<?> ignore) {
		conf.setInputFormat(AvroInputFormat.class);
		conf.set(AvroJob.INPUT_IS_REFLECT, "true");
	}

	public String fileExtension() {
		return ".avro";
	}

	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.AVRO_FORMAT);
	}
	
	
	/**
	 * Compare file signature of Avro type file (uncompressed)
	 */
	@Override
	public boolean signature(byte[] header) {
		return Arrays.equals(FILE_SIGNATURE, Arrays.copyOfRange(header, 0, FILE_SIGNATURE.length));
	}

    @Override
    public boolean isCompatible(InputFormat foramt) {
        // TODO Auto-generated method stub
        return true;
    }

}
