/**
 * 
 */
package tap;

import org.apache.hadoop.conf.Configuration;

/**
 * Encapsulate Pipe status functionality
 */
public class PipeStat {
	DFSStat stat = null;
	
	PipeStat(String path, Configuration conf) {
		stat = new DFSStat(path, conf);
	}
	
	

}
