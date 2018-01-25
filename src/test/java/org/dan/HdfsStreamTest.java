package org.dan;

import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * 用流的方式来操作hdfs上的文件
 * 可以实现读写指定偏移量范围的数据
 * @author ctbri
 *
 */
public class HdfsStreamTest {
	
	private FileSystem fs;
	private Configuration configuration;
	
	@Before
	public void init() throws Exception {
		configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://219.141.189.148:9000");
		fs = FileSystem.get(configuration);
	}
	
	@Test
	public void testUpload() throws Exception {
		FSDataOutputStream outputStream = fs.create(new Path("/stream_test"), true);
		FileInputStream inputStream = new FileInputStream("/Users/ctbri/yingtan_trashcan_real.jar");
		IOUtils.copy(inputStream, outputStream);
		
	}
	
	@Test
	public void testDownload() throws Exception {
		FSDataInputStream inputStream = fs.open(new Path("/stream_test"));
		FileOutputStream outStream = new FileOutputStream("/Users/ctbri/yingtan_trashcan_real1.jar");
		IOUtils.copy(inputStream, outStream);
	}
	
	@Test
	public void testRandomAccess() throws Exception {
		FSDataInputStream inputStream = fs.open(new Path("/stream_test"));
		FileOutputStream outStream = new FileOutputStream("/Users/ctbri/yingtan_trashcan_real1.jar");
		IOUtils.copyLarge(inputStream, outStream, 1024, 1024);
	}
	
	@Test
	public void testCat() throws Exception {
		FSDataInputStream inputStream = fs.open(new Path("/stream_test"));
		IOUtils.copyLarge(inputStream, System.out, 1024, 1024);
	}
	
}
