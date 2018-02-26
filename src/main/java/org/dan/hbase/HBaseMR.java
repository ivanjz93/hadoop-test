package org.dan.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.BasicConfigurator;

/**
 * 
 *	@author jiangzhi
 *	@version 创建时间：2018年2月26日上午11:21:25
 *	类说明：MapReduce操作HBase实现word count
 */
public class HBaseMR {

	private static Configuration config = null;
	private static Connection connection = null;

	
	private static final String TABLE_NAME_STR = "word";
	private static final String COL_FAMILY = "content";
	private static final String COL_NAME = "info";
	private static final String TABLE_NAME_STR_2 = "stat";
	
	private static TableName tableName1;
	private static TableName tableName2;
	
	private static Table infoTable;
	//private static Table statTable;
	
	public static void init() throws IOException {
		config = HBaseConfiguration.create();
		config.set("mapreduce.framework.name", "yarn");
		config.set("yarn.resourcemanager.hostname", "nn1");
		//config.set("hbase.zookeeper.quorum", "nn1-ha,nn2,nn2-ha");
		//config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("fs.defaultFS", "hdfs://master1/");
		connection = ConnectionFactory.createConnection(config);
		tableName1 = TableName.valueOf(TABLE_NAME_STR);
		tableName2 = TableName.valueOf(TABLE_NAME_STR_2);
		infoTable = connection.getTable(tableName1);
		//statTable = connection.getTable(tableName2);
		initDB();
	}
	
	public static void initDB() throws IOException {
		Admin admin = connection.getAdmin();
		if(admin.tableExists(tableName1)
				|| admin.tableExists(tableName2)) {
			admin.disableTable(tableName1);
			admin.deleteTable(tableName1);
			admin.disableTable(tableName2);
			admin.deleteTable(tableName2);
		}
		HTableDescriptor desc1 = new HTableDescriptor(tableName1);
		HColumnDescriptor family1 = new HColumnDescriptor(COL_FAMILY);
		desc1.addFamily(family1);
		admin.createTable(desc1);
		
		HTableDescriptor desc2 = new HTableDescriptor(tableName2);
		HColumnDescriptor family2 = new HColumnDescriptor(COL_FAMILY);
		desc2.addFamily(family2);
		admin.createTable(desc2);
		
		List<Put> puts = new ArrayList<>();
		Put put = new Put(Bytes.toBytes("0"));
		put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_NAME), Bytes.toBytes("Linear and modular scalability"));
		Put put1 = new Put(Bytes.toBytes("1"));
		put1.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_NAME), Bytes.toBytes("Strictly consistent reads and writes"));
		Put put2 = new Put(Bytes.toBytes("2"));
		put2.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_NAME), Bytes.toBytes("Automatic and configurable sharding of tables"));
		Put put3 = new Put(Bytes.toBytes("3"));
		put3.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_NAME), Bytes.toBytes("Automatic failover support between RegionServers"));
		Put put4 = new Put(Bytes.toBytes("4"));
		put4.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_NAME), Bytes.toBytes("Convenient base classes for backing Hadoop MapReduce jobs with Apache HBase tables"));
		Put put5 = new Put(Bytes.toBytes("5"));
		put5.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_NAME), Bytes.toBytes("Easy to use Java API for client access"));
		puts.add(put);
		puts.add(put1);
		puts.add(put2);
		puts.add(put3);
		puts.add(put4);
		puts.add(put5);
		infoTable.put(puts);
	}
	
	public static class MyMapper extends TableMapper<Text, IntWritable> {
		private static IntWritable outValue = new IntWritable(1);
		private static Text outKey = new Text();
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			String line = Bytes.toString(value.getValue(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_NAME)));
			String[] words = line.split(" ");
			for(String word : words) {
				outKey.set(word);
				context.write(outKey, outValue);
			}
		}
	}
	
	public static class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values) {
				sum += value.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_NAME), Bytes.toBytes(String.valueOf(sum)));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
		}
		
	}
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		init();
		Job job = Job.getInstance(config);
		job.setJarByClass(HBaseMR.class);
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_NAME));
		TableMapReduceUtil.initTableMapperJob(TABLE_NAME_STR, scan, MyMapper.class, Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(TABLE_NAME_STR_2, MyReducer.class, job);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
