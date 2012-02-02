package tap.formats.json;

import tap.core.Pipe;
import tap.formats.Formats;
import tap.formats.text.TextFormat;

public class JsonFormat extends TextFormat {

	@Override
	public String fileExtension() {
		return ".json";
	}

	@Override
	public void setPipeFormat(Pipe pipe) {
		pipe.setFormat(Formats.JSON_FORMAT);
	}

	/**
	 * matches if starts with open bracket { (ignoring whitespace)
	 */
	@Override
	public boolean signature(byte[] header) {
		if (super.signature(header)) {
			String s = new String(header);
			if (s.trim().startsWith("{")) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public boolean instanceOfCheck(Object o) {
		return false;
	}

}