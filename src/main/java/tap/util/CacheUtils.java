/**
 * 
 */
package tap.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

/**
 *
 */
public class CacheUtils {

	private static String getTmpFilepath(String key) {
		return "/tmp/tap.mapper-param." + key; // TODO: needs to be a bit more robust
	}

	/**
	 * Create tmp file, serialize value into file, then add file to distributed
	 * cache.
	 * 
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	public static void addMapToCache(String key, Serializable value, Configuration conf)
			throws IOException {
		File f = new File(getTmpFilepath(key));
		FileOutputStream fos = new FileOutputStream(f);
		ObjectOutputStream out = new ObjectOutputStream(fos);
		out.writeObject(value);
		out.close();
		fos.close();
		DistributedCache.addCacheFile(f.toURI(), conf);
		f.deleteOnExit(); // remove tmp file
	}
	
	/**
	 * De-serialize object from Distributed Cache
	 * @param key
	 * @param conf
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object getFromCache(String key, Configuration conf) throws IOException, ClassNotFoundException {
		Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		if (null != cacheFiles && cacheFiles.length > 0) {
			for (Path path : cacheFiles) {
				if (path.getName().contains(key)) {
					ObjectInputStream ois = new ObjectInputStream(
							new FileInputStream(path.toString()));
					Object rc = ois.readObject();
					ois.close();
					return rc;
				}
			}
		}
		return null;
	}
}
