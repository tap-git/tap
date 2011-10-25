package tap.formats.text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import tap.core.Pipe;
import tap.formats.FileFormat;
import tap.formats.Formats;

@SuppressWarnings("deprecation")
public class TextFormat extends FileFormat {

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
		return ".txt";
	}

	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.STRING_FORMAT);
	}

	/**
	 * Should contain one new line and range of characters are printable
	 */
	@Override
	public boolean signature(byte[] header) {

		boolean hasRecordDeliminator = false;
		boolean hasPrintableOnly = false;
		
		if (header[0] == 0xFF) {
			// Unicode ?
		} else {
			for (byte b : header) {
				if (b == '\n') {
					hasRecordDeliminator = true;
					continue;
				} else if (b == '\t' || b >= 32 && b < 127) {
					continue;
				}
				hasPrintableOnly = false;
				break;
			}
			hasPrintableOnly = true;
		}
		return hasRecordDeliminator && hasPrintableOnly;
	}
}