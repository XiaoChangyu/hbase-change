package test;


import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

/**
 * 功能描述: 测试设置 hostname - ip 映射的方式进行 DDL 和 DML 操作.
 * @createTime: 2016年9月14日 下午2:32:33
 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @version: 0.1
 * @lastVersion: 0.1
 * @updateTime: 2016年9月14日 下午2:32:33
 * @updateAuthor: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @changesSum:
 */
public class TestSetHostnameIp {

	public static void main(String[] args) throws Throwable {
		Configuration conf = new Configuration();
		conf.set("hbase.cluster.distributed", "true");
		conf.set("hbase.zookeeper.quorum", "zookeeper1.bigdata,zookeeper2.bigdata,zookeeper3.bigdata");
		conf.set("hbase.master.info.port", "60010");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		ConnectionFactory.setHost("zookeeper1.bigdata", "192.168.1.10");
		ConnectionFactory.setHost("zookeeper2.bigdata", "192.168.1.11");
		ConnectionFactory.setHost("zookeeper3.bigdata", "192.168.1.12");
		ConnectionFactory.setHost("namenode1.bigdata", "192.168.1.20");
		ConnectionFactory.setHost("namenode2.bigdata", "192.168.1.21");
		ConnectionFactory.setHost("datanode1.bigdata", "192.168.1.22");
		ConnectionFactory.setHost("datanode2.bigdata", "192.168.1.23");
		ConnectionFactory.setHost("datanode3.bigdata", "192.168.1.24");
		ConnectionFactory.setHost("datanode4.bigdata", "192.168.1.25");

		Connection connection = ConnectionFactory.createConnection(conf);

		/* DDL */
		listTable(connection);

		System.out.println("-----------------");
		String createTableName = "test_hbase";
		String createTableFamily = "0";
		createTable(connection, createTableName, createTableFamily);

		System.out.println("-----------------");
		String disableTableName = "test_hbase";
		disableTable(connection, disableTableName);

		System.out.println("-----------------");
		String deleteTableName = "test_hbase";
		deleteTable(connection, deleteTableName);

		/* DML */
		System.out.println("-----------------");
		String putTableName = "test_hbase";
		String putRowkey = "BBBBB";
		String putQualifier = "B";
		String putValue = "bbbbb";
		testPut(connection, putTableName, putRowkey, putQualifier, putValue);

		System.out.println("-----------------");
		String scanTableName = "test_hbase";
		String scanStartRowkey = "";
		String scanStopRowkey = "";
		testScan(connection, scanTableName, scanStartRowkey, scanStopRowkey);

		System.out.println("-----------------");
		String getTableName = "test_hbase";
		String getRowkey = "AAAAA";
		testGet(connection, getTableName, getRowkey);

		connection.close();
	}

	public static void listTable(Connection connection) throws Throwable {
		Admin admin = connection.getAdmin();
		HTableDescriptor[] listTables = admin.listTables();
		for (HTableDescriptor tableDesc: listTables) {
			String tableName = tableDesc.getTableName().getNameAsString();
			System.out.print("LISTTABLE TABLENAME: " + tableName);
			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
			for (HColumnDescriptor columnDesc: columnFamilies) {
				String family = columnDesc.getNameAsString();
				System.out.print(" FAMILY: " + family);
			}
			System.out.println();
		}
		admin.close();
	}

	public static void createTable(Connection connection, String tableName, String family) throws Throwable {
		Admin admin = connection.getAdmin();
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		tableDescriptor.addFamily(new HColumnDescriptor(family.getBytes()));
		admin.createTable(tableDescriptor);
		System.out.println("CreateTable " + tableName + " " + family);
		admin.close();
	}

	public static void disableTable(Connection connection, String tableName) throws Throwable {
		Admin admin = connection.getAdmin();
		admin.disableTable(TableName.valueOf(tableName));
		System.out.println("DisableTable " + tableName);
		admin.close();
	}

	public static void deleteTable(Connection connection, String tableName) throws Throwable {
		Admin admin = connection.getAdmin();
		admin.deleteTable(TableName.valueOf(tableName));
		System.out.println("DeleteTable " + tableName);
		admin.close();
	}

	public static void testPut(Connection connection, String tableName, String rowkey, String qualifier, String value) throws Throwable {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowkey.getBytes());
		put.addColumn("0".getBytes(), qualifier.getBytes(), value.getBytes());
		table.put(put);
		System.out.println("Put " + new String(rowkey) + " " +
				new String("0") + ":" + new String(qualifier) + " " + new String(value));
		table.close();
	}

	public static void testScan(Connection connection, String tableName, String startRowkey, String stopRowkey) throws Throwable {
		Scan scan = new Scan();
		scan.setStartRow(startRowkey.getBytes());
		scan.setStopRow(stopRowkey.getBytes());
		Table table = connection.getTable(TableName.valueOf(tableName));
		ResultScanner results = table.getScanner(scan);
		Result result = null;
		long count = 0;
		while ((result = results.next()) != null) {
			List<Cell> listCells = result.listCells();
			byte[] rowkey = result.getRow();
			if (listCells != null && listCells.size() > 0) {
				for (Cell cell: listCells) {
					byte[] family = CellUtil.cloneFamily(cell);
					byte[] qualifier = CellUtil.cloneQualifier(cell);
					byte[] value = CellUtil.cloneValue(cell);
					long timestamp = cell.getTimestamp();
					System.out.println("Scan " + new String(rowkey) + " " +
								new String(family) + ":" + new String(qualifier) + " " +
								timestamp + " " + new String(value));
				}
				count+=1;
			}
		}
		System.out.println("Count: " + count);
		table.close();
	}

	public static void testGet(Connection connection, String tableName, String rowkey) throws Throwable {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Result result = table.get(new Get(rowkey.getBytes()));
		List<Cell> listCells = result.listCells();
		if (listCells != null && listCells.size() > 0) {
			for (Cell cell: listCells) {
				byte[] family = CellUtil.cloneFamily(cell);
				byte[] qualifier = CellUtil.cloneQualifier(cell);
				byte[] value = CellUtil.cloneValue(cell);
				long timestamp = cell.getTimestamp();
				System.out.println("Get " + new String(rowkey) + " " +
							new String(family) + ":" + new String(qualifier) + " " +
							timestamp + " " + new String(value));
			}
		}
		table.close();
	}
}
