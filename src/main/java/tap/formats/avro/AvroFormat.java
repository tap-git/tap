package tap.formats.avro;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.mapred.JobConf;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class AvroFormat extends FileFormat {

	public void setupOutput(JobConf conf) {
		conf.setOutputFormat(AvroOutputFormat.class);
		conf.setOutputKeyClass(AvroWrapper.class);
	}

	public void setupInput(JobConf conf) {
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

}
