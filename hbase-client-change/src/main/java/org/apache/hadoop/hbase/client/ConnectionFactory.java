/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;


/**
 * A non-instantiable class that manages creation of {@link Connection}s.
 * Managing the lifecycle of the {@link Connection}s to the cluster is the responsibility of
 * the caller.
 * From a {@link Connection}, {@link Table} implementations are retrieved
 * with {@link Connection#getTable(TableName)}. Example:
 * <pre>
 * Connection connection = ConnectionFactory.createConnection(config);
 * Table table = connection.getTable(TableName.valueOf("table1"));
 * try {
 *   // Use the table as needed, for a single operation and a single thread
 * } finally {
 *   table.close();
 *   connection.close();
 * }
 * </pre>
 *
 * Similarly, {@link Connection} also returns {@link Admin} and {@link RegionLocator}
 * implementations.
 *
 * This class replaces {@link HConnectionManager}, which is now deprecated.
 * @see Connection
 * @since 0.99.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ConnectionFactory {

	/**
	 * 保存 hostname 与 IP 对应关系的 map. 因为是 static, 所以隔离级别是进程级的.
	 */
	private static ConcurrentHashMap<String, String> hostnameIpMap = new ConcurrentHashMap<String, String>();

	/**
	 * 设置 hostname 与 IP 对应关系. <br/>
	 * 这些关系会在 HBase-client 访问 HBase 的 ZooKeeper 集群, HMaster, HRegionServer 时进行转换. <br/>
	 * 如果这里进行了设置, 则可以不用在调用 HBase-client 的客户端服务器配置 hosts 映射.
	 * @createTime: 2016年9月13日 下午5:41:50
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param hostname
	 * @param ip
	 */
	public static void setHost(String hostname, String ip) {
		hostnameIpMap.put(hostname, ip);
	}

	/**
	 * 获取 hostname 对应 IP. <br/>
	 * 具体 hostname 与 IP 对应关系需要事先调用 {@link #setHost(String, String)} 进行设置.
	 * @createTime: 2016年9月13日 下午5:40:16
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param hostname
	 * @return {@link String}
	 */
	public static String getHostnameTargetIp(String hostname) {
		String ip = hostnameIpMap.get(hostname);
		if (ip != null) return ip;
		return hostname;
	}

	/**
	 * 专为  ZooKeeper 集群服务器地址使用的, 获取 hostname 对应 IP. <br/>
	 * 若传入参数为 <code>zookeeper1.bigdata,zookeeper2.bigdata,zookeeper3.bigdata</code>, <br/>
	 * 而 zookeeper1.bigdata, zookeeper2.bigdata, zookeeper3.bigdata 分别对应 IP 是 192.168.0.100, 192.168.0.101, 192.168.0.102. <br/>
	 * 则转换后为 <code>192.168.0.100,192.168.0.100,192.168.0.100</code> <br/>
	 * <br/>
	 * 若传入参数为 <code>zookeeper1.bigdata:2181,zookeeper2.bigdata:2181,zookeeper3.bigdata:2181</code>, <br/>
	 * 而 zookeeper1.bigdata, zookeeper2.bigdata, zookeeper3.bigdata 分别对应 IP 是 192.168.0.100, 192.168.0.101, 192.168.0.102. <br/>
	 * 则转换后为 <code>192.168.0.100:2181,192.168.0.100:2181,192.168.0.100:2181</code> <br/>
	 * 具体 hostname 与 IP 对应关系需要事先调用 {@link #setHost(String, String)} 进行设置.
	 * @createTime: 2016年9月13日 下午5:36:04
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param quorumServers
	 * @return {@link String}
	 */
	public static String getHostnameTargetIpForZooKeeper(String quorumServers) {
		String[] quorumServerArray = quorumServers.split(",");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < quorumServerArray.length; i++) {
			if (i > 0) sb.append(",");
			String[] quorum = quorumServerArray[i].split(":");
			if (quorum.length == 2) {
				sb.append(getHostnameTargetIp(quorum[0].trim())).append(":").append(quorum[1]);
			} else if (quorum.length == 1) {
				sb.append(getHostnameTargetIp(quorum[0].trim()));
			}
		}
		return sb.toString();
	}

  /** No public c.tors */
  protected ConnectionFactory() {
  }

  /**
   * Create a new Connection instance using default HBaseConfiguration. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection();
   * Table table = connection.getTable(TableName.valueOf("mytable"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection() throws IOException {
    return createConnection(HBaseConfiguration.create(), null, null);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("mytable"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @param conf configuration
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf) throws IOException {
    return createConnection(conf, null, null);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("mytable"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @param conf configuration
   * @param pool the thread pool to use for batch operations
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, ExecutorService pool)
      throws IOException {
    return createConnection(conf, pool, null);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("table1"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @param conf configuration
   * @param user the user the connection is for
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, User user)
  throws IOException {
    return createConnection(conf, null, user);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("table1"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @param conf configuration
   * @param user the user the connection is for
   * @param pool the thread pool to use for batch operations
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, ExecutorService pool, User user)
  throws IOException {
    if (user == null) {
      UserProvider provider = UserProvider.instantiate(conf);
      user = provider.getCurrent();
    }

    return createConnection(conf, false, pool, user);
  }

  static Connection createConnection(final Configuration conf, final boolean managed,
      final ExecutorService pool, final User user)
  throws IOException {
    String className = conf.get(HConnection.HBASE_CLIENT_CONNECTION_IMPL,
      ConnectionManager.HConnectionImplementation.class.getName());
    Class<?> clazz = null;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    try {
      // Default HCM#HCI is not accessible; make it so before invoking.
      Constructor<?> constructor =
        clazz.getDeclaredConstructor(Configuration.class,
          boolean.class, ExecutorService.class, User.class);
      constructor.setAccessible(true);
      return (Connection) constructor.newInstance(conf, managed, pool, user);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
