package tap;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

public class DFSStatTests {

	@Test
	public void testSingleFile() {
		DFSStat stat = new DFSStat("share/decameron.txt", new JobConf());
		Assert.assertNotNull(stat.path);
		Assert.assertNotNull(stat.conf);
		Assert.assertNotNull(stat.dfsPath);
		Assert.assertNotNull(stat.fs);
		Assert.assertTrue(stat.isFile);
		Assert.assertTrue(stat.exists);
		Assert.assertTrue(0<stat.timestamp);
		Assert.assertNotNull(stat.getStatuses());
		Assert.assertEquals(1,stat.getStatuses().length);
		Assert.assertFalse(stat.getStatuses()[0].isDir());
	}
	
	@Test
	public void testDirectory() {
		DFSStat stat = new DFSStat("share/multi/01", new JobConf());
		Assert.assertNotNull(stat.path);
		Assert.assertNotNull(stat.conf);
		Assert.assertNotNull(stat.dfsPath);
		Assert.assertNotNull(stat.fs);
		Assert.assertFalse(stat.isFile);
		Assert.assertTrue(stat.exists);
		Assert.assertTrue(0<stat.timestamp);

		Assert.assertNotNull(stat.getStatuses());
		Assert.assertEquals(4,stat.getStatuses().length);
		
		// doesn't have sub-dirs
		for(FileStatus fs: stat.getStatuses()) {
			Assert.assertFalse(fs.isDir());
		}
	}
	
	@Test
	public void testRecursiveDirectory() {
		DFSStat stat = new DFSStat("share/multi", new JobConf());
		Assert.assertNotNull(stat.path);
		Assert.assertNotNull(stat.conf);
		Assert.assertNotNull(stat.dfsPath);
		Assert.assertNotNull(stat.fs);
		Assert.assertFalse(stat.isFile);
		Assert.assertTrue(stat.exists);
		Assert.assertTrue(0<stat.timestamp);

		Assert.assertNotNull(stat.getStatuses());
		Assert.assertEquals(14,stat.getStatuses().length);
		
		// doesn't have sub-dirs
		boolean hasSubdirs = false;
		for(FileStatus fs: stat.getStatuses()) {
			if (fs.isDir()) 
				hasSubdirs = true;
		}
		Assert.assertTrue(hasSubdirs);
	}

	@Test
	public void testMissing() {
		DFSStat stat = new DFSStat("share/missing.txt", new JobConf());
		Assert.assertNotNull(stat.path);
		Assert.assertNotNull(stat.conf);
		Assert.assertNotNull(stat.dfsPath);
		Assert.assertNotNull(stat.fs);
		Assert.assertFalse(0<stat.timestamp);
		Assert.assertFalse(stat.exists);
	}
}
