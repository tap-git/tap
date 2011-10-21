package tap.formats.json;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class JsonFormat extends FileFormat {

	@Override
	public void setupOutput(JobConf conf) {
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(String.class);
	}

	@Override
	public void setupInput(JobConf conf) {
		conf.setInputFormat(TextInputFormat.class);
	}

	@Override
	public String fileExtension() {
		return ".json";
	}

	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.JSON_FORMAT);
	}

}
