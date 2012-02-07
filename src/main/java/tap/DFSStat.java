package tap;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DFSStat  {
	Path dfsPath = null;
	FileSystem fs = null;
	boolean exists = false;
	boolean isObsolete = true;
	boolean isFile = false;
	long timestamp = 0;
	private FileStatus[] statuses = null;
	
	String path;
	Configuration conf;
	DFSStat(String path, Configuration conf) {
		if (null == path) {
			throw new IllegalArgumentException("null path");
		}
		if (null == conf) {
			throw new IllegalArgumentException("null conf");
		}
		this.path = path;
		this.conf = conf;
		init();
	}

	private void init() {
		dfsPath = new Path(path);
		try {
			fs = dfsPath.getFileSystem(conf);
			if (null != fs) {
				exists = fs.exists(dfsPath);
				if (exists) {
					isFile = fs.isFile(dfsPath);
					status = fs.getFileStatus(dfsPath);
					timestamp = status.getModificationTime();
				}
			}
		} catch (FileNotFoundException e) {
			exists = false;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	FileStatus status = null;

	FileStatus[] getStatuses() {
		if (null == statuses) {
			try {
				statuses = fs.listStatus(dfsPath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return statuses;
	}

	void setStatuses(FileStatus[] statuses) {
		this.statuses = statuses;
	}
}