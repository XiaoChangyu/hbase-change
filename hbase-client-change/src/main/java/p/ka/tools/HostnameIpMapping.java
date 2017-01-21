package p.ka.tools;



import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.Arrays;

/**
 * 功能描述: 设置 hostname 和 IP 的映射关系的工具类
 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @version: 0.1
 */
public class HostnameIpMapping {

	/**
	 * 功能描述: Test
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param args
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		HostnameIpMapping.setHost2Ip("zookeeper1.bigdata", new byte[] {(byte) 192, (byte) 168, 1, 10});
		HostnameIpMapping.setHost2Ip("zookeeper2.bigdata", new byte[] {(byte) 192, (byte) 168, 1, 11});
		HostnameIpMapping.setHost2Ip("zookeeper3.bigdata", new byte[] {(byte) 192, (byte) 168, 1, 12});
		HostnameIpMapping.setHost2Ip("namenode1.bigdata", new byte[] {(byte) 192, (byte) 168, 1, 20});
		HostnameIpMapping.setHost2Ip("namenode2.bigdata", new byte[] {(byte) 192, (byte) 168, 1, 21});
		HostnameIpMapping.setHost2Ip("datanode1.bigdata", "192.168.1.22");
		HostnameIpMapping.setHost2Ip("datanode2.bigdata", "192.168.1.23");
		HostnameIpMapping.setHost2Ip("datanode3.bigdata", "192.168.1.24");
		HostnameIpMapping.setHost2Ip("datanode4.bigdata", "192.168.1.25");

		InetAddress[] cachedAddresses = getCachedAddresses("zookeeper3.bigdata");
		System.out.println(cachedAddresses == null ? null : Arrays.asList(cachedAddresses));
		cachedAddresses = getCachedAddresses("datanode2.bigdata");
		System.out.println(cachedAddresses == null ? null : Arrays.asList(cachedAddresses));

		java.net.InetSocketAddress isa = new java.net.InetSocketAddress("zookeeper3.bigdata", 888);
		System.out.println(isa.getAddress().getHostAddress());
		isa = new java.net.InetSocketAddress("datanode2.bigdata", 888);
		System.out.println(isa.getAddress().getHostAddress());
	}





	private final static Method cacheAddresses;
	private final static Method getCachedAddresses;

	static {
		Method _cacheAddresses = null;
		Method _getCachedAddresses = null;
		try {
			_cacheAddresses = InetAddress.class.getDeclaredMethod("cacheAddresses", String.class, InetAddress[].class, boolean.class);
			_cacheAddresses.setAccessible(true);
			_getCachedAddresses = InetAddress.class.getDeclaredMethod("getCachedAddresses", String.class);
			_getCachedAddresses.setAccessible(true);
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		cacheAddresses = _cacheAddresses;
		getCachedAddresses = _getCachedAddresses;
	}

	/**
	 * 功能描述: 缓存地址信息, 效果和 {@link InetAddress#cacheAddresses(String, InetAddress[], boolean)} 相同.
	 * Cache the given hostname and addresses.
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param hostname
	 * @param addresses
	 * @param success
	 * @throws Throwable
	 */
	public static synchronized void cacheAddresses(String hostname, InetAddress[] addresses, boolean success) throws Throwable {
		Method _cacheAddresses = cacheAddresses;
		if (_cacheAddresses == null) throw new RuntimeException("Method cacheAddresses has not init");
		_cacheAddresses.invoke(InetAddress.class, hostname, addresses, success);
	}

	/**
	 * 功能描述: 获取已缓存的地址信息, 效果和 {@link InetAddress#getCachedAddresses(String)} 相同.
	 * Lookup hostname in cache (positive & negative cache). If found return addresses, null if not found.
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param hostname
	 * @return {@link InetAddress[]}
	 * @throws Throwable
	 */
	public static synchronized InetAddress[] getCachedAddresses(String hostname) throws Throwable {
		Method _getCachedAddresses = getCachedAddresses;
		if (_getCachedAddresses == null) throw new RuntimeException("Method cacheAddresses has not init");
		return (InetAddress[]) _getCachedAddresses.invoke(InetAddress.class, hostname);
	}


	/**
	 * 功能描述: 设置 hostname 和 IP 的映射关系. addr 是 IPv4 的字节表示形式.
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param hostname
	 * @param addr
	 * @throws Throwable
	 */
	public static synchronized void setHost2Ip(String hostname, byte[] addr) throws Throwable {
		InetAddress[] cachedAddresses = getCachedAddresses(hostname);
		if (cachedAddresses == null) {
			InetAddress ret = InetAddress.getByAddress(hostname, addr);
			cacheAddresses(hostname, new InetAddress[] {ret}, true);
		}
	}

	/**
	 * 功能描述: 设置 hostname 和 IP 的映射关系. ipv4 是 IPv4 地址的字符串形式.
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param hostname
	 * @param ipv4
	 * @throws Throwable
	 */
	public static synchronized void setHost2Ip(String hostname, String ipv4) throws Throwable {
		InetAddress[] cachedAddresses = getCachedAddresses(hostname);
		if (cachedAddresses == null) {
			if (ipv4 == null || ipv4.length() < 7)
				throw new RuntimeException("Error ipv4: " + ipv4);
			String[] addrs = ipv4.split("\\.");
			if (addrs.length != 4)
				throw new RuntimeException("Error ipv4: " + ipv4);
			byte[] addr = new byte[4];
			for (int i = 0; i < addr.length; i++) {
				int ai = Integer.parseInt(addrs[i]);
				if (ai < 0 || ai > 255)
					throw new RuntimeException("Error ipv4: " + ipv4);
				addr[i] = (byte) ai;
			}
			InetAddress ret = InetAddress.getByAddress(hostname, addr);
			cacheAddresses(hostname, new InetAddress[] {ret}, true);
		}
	}
}
