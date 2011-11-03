package tap.compression.gz;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

import tap.compression.Compressions;
import tap.compression.FileCompression;
import tap.core.Pipe;

@SuppressWarnings("deprecation")
public class GzipCompression extends FileCompression {

	public GzipCompression() {
		super(GzipCodec.class);
	}
	@Override
	public String fileExtension() {
		return "gz";
	}

	@Override
	public void setPipeCompression(Pipe pipe) {
		pipe.setCompression(Compressions.GZIP_COMPRESSION);
		
	}

	@Override
	public void setupInput(JobConf conf) {
		//NOOP Gzip Input format is already supported by default
		
	}

	@Override
	public void setupOutput(JobConf conf) {
		TextOutputFormat.setCompressOutput(conf, true);
		TextOutputFormat.setOutputCompressorClass(conf, this.codec);
		
	}
	@Override
	public Class<? extends CompressionCodec> getCodec() {
		return this.codec;
	}

}
