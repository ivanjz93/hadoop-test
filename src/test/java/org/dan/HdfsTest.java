package org.dan;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsTest {

	FileSystem fs;
	Configuration configuration;
	@Before
	public void init() throws Exception {
		configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://219.141.189.148:9000");
		fs = FileSystem.get(configuration);
	}
	
	@Test
	public void testUpload() throws Exception {
		fs.copyFromLocalFile(new Path("/Users/ctbri/test.txt"), new Path("/test.txt.copy"));
		fs.close();
	}
	
	@Test
	public void testDownload() throws Exception {
		fs.copyToLocalFile(new Path("/test.txt.copy"), new Path("/Users/ctbri/"));
		fs.close();
	}
	
	@Test
	public void testConf() {
		Iterator<Entry<String, String>> confIt = configuration.iterator();
		while(confIt.hasNext()) {
			Entry<String, String> entry = confIt.next();
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
	}
	
	@Test
	public void testMkdir() throws Exception {
		boolean mkdirs = fs.mkdirs(new Path("/testmkdir"));
		//fs.close();
		assertTrue(mkdirs);
	}
	
	@Test
	public void testDelete() throws Exception {
		boolean delete = fs.delete(new Path("/testmkdir"), true);
		assertTrue(delete);
	}
	
	@Test
	public void testLs() throws Exception {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println("Name: " + fileStatus.getPath().getName());
			System.out.println("blocksize: " + fileStatus.getBlockSize());
			System.out.println("owner: " + fileStatus.getOwner());
			System.out.println("Replication: " + fileStatus.getReplication());
			System.out.println("Permission: " + fileStatus.getPermission());
			for(BlockLocation bl : fileStatus.getBlockLocations()) {
				System.out.println("Block Offset: " + bl.getOffset());
				System.out.println("Block Length: " + bl.getLength());
				System.out.println("Block Hosts: " + Arrays.toString(bl.getHosts()));
			}
			System.out.println("-----------------------------------");
		}
	}
	
	@Test
	public void testLs2() throws Exception {
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus fileStatus : listStatus) {
			System.out.println("Name: " + fileStatus.getPath().getName());
			System.out.println("blocksize: " + fileStatus.getBlockSize());
			System.out.println("owner: " + fileStatus.getOwner());
			System.out.println("Replication: " + fileStatus.getReplication());
			System.out.println("Permission: " + fileStatus.getPermission());
			System.out.println(fileStatus.isFile() ? "File" : "Directory");
			System.out.println("-----------------------------------");
		}
	}
}
