package org.dan;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 *	@author jiangzhi
 *	@version 创建时间：2018年2月24日下午2:08:33
 *	类说明：HBase java api测试类
 */
public class HBaseTest {
	private Configuration config = null;
	private Connection connection = null;
	private Table table;
	
	@Before
	public void init() throws Exception {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "nn1-ha,nn2,nn2-ha");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		connection = ConnectionFactory.createConnection(config);
		table = connection.getTable(TableName.valueOf("user"));
	}
	
	@Test
	public void createTable() throws Exception{
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf("test3");
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor family = new HColumnDescriptor("info");
		HColumnDescriptor family2 = new HColumnDescriptor("info2");
		desc.addFamily(family);
		desc.addFamily(family2);
		admin.createTable(desc);
	}
	
	@Test
	public void insertData() throws Exception {
		List<Put> puts = new ArrayList<>();
		Put put = new Put(Bytes.toBytes("jj_200"));
		put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
		put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("sex"), Bytes.toBytes("0"));
		put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("23"));
		put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("address"), Bytes.toBytes("asdasfdf"));
		Put put1 = new Put(Bytes.toBytes("jj_201"));
		put1.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
		put1.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("sex"), Bytes.toBytes("1"));
		put1.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("32"));
		put1.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("address"), Bytes.toBytes("adfds"));
		puts.add(put);
		puts.add(put1);
		table.put(puts);
	}
	
	@Test
	public void deleteData() throws Exception {
		Delete delete = new Delete(Bytes.toBytes("jj_201"));
		delete.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));
		table.delete(delete);
	}
	
	@Test
	public void queryData() throws Exception {
		Get get = new Get(Bytes.toBytes("jj_200"));
		//get.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));
		
		Result result = table.get(get);
		for(Cell cell : result.rawCells()) {
			System.out.print(Bytes.toString(CellUtil.cloneRow(cell)) + " ");
			System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + " ");
			System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + " ");
			System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println();
		}
	}
	
	@Test
	public void scanData() throws Exception {
		Scan scan = new Scan();
		scan.withStartRow(Bytes.toBytes("jj_199"));
		scan.withStopRow(Bytes.toBytes("jj_202"));
		//scan.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("address"));
		
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			for(Cell cell : result.rawCells()) {
				System.out.print(Bytes.toString(CellUtil.cloneRow(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println();
			}
			System.out.println("================");
		}
	}
	
	@Test
	public void scanDataByFilter1() throws Exception {
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info1"), Bytes.toBytes("sex"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("0"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			for(Cell cell : result.rawCells()) {
				System.out.print(Bytes.toString(CellUtil.cloneRow(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println();
			}
			System.out.println("================");
		}
	}
	
	@Test
	public void scanDataByFilter2() throws Exception {
		ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("na"));
		Scan scan = new Scan();
		scan.setFilter(columnPrefixFilter);
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			for(Cell cell : result.rawCells()) {
				System.out.print(Bytes.toString(CellUtil.cloneRow(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println();
			}
			System.out.println("================");
		}
	}
	
	@Test
	public void scanDataByFilter3() throws Exception {
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info1"), Bytes.toBytes("sex"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("0"));
		ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("na"));
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			for(Cell cell : result.rawCells()) {
				System.out.print(Bytes.toString(CellUtil.cloneRow(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + " ");
				System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println();
			}
			System.out.println("================");
		}
	}
	
	@After
	public void destroy() throws Exception{
		table.close();
		connection.close();
		
	}
}
