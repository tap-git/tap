package tap.formats.record;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class RecordFormat extends FileFormat {

	@Override
	public void setupOutput(JobConf conf, Class<?> ignore) {
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(String.class);
	}

	@Override
	public void setupInput(JobConf conf, Class<?> ignore) {
		conf.setInputFormat(TextInputFormat.class);  
	}

	@Override
	public String fileExtension() {
		// TODO Auto-generated method stub
		return ".tsv";
	}

	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.RECORD_FORMAT);
	}

    @Override
    public boolean isCompatible(InputFormat format) {
        // TODO Auto-generated method stub
        return false;
    }

}
