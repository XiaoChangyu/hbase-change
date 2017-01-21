package p.ka.tools;

import java.net.InetAddress;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import p.ka.tools.HostnameIpMapping;

public class TestHostnameIpMapping {

	@Test
	public void test() throws Throwable {
		HostnameIpMapping.setHost2Ip("zookeeper3.bigdata", new byte[] {(byte) 192, (byte) 168, 1, 12});
		HostnameIpMapping.setHost2Ip("datanode2.bigdata", "192.168.1.23");

		InetAddress[] cachedAddresses = HostnameIpMapping.getCachedAddresses("zookeeper3.bigdata");
		System.out.println(cachedAddresses == null ? null : Arrays.asList(cachedAddresses));
		cachedAddresses = HostnameIpMapping.getCachedAddresses("datanode2.bigdata");
		System.out.println(cachedAddresses == null ? null : Arrays.asList(cachedAddresses));

		java.net.InetSocketAddress isa = new java.net.InetSocketAddress("zookeeper3.bigdata", 888);
		System.out.println(isa.getAddress().getHostAddress());
		Assert.assertEquals(isa.getAddress().getHostAddress(), "192.168.1.12");

		isa = new java.net.InetSocketAddress("datanode2.bigdata", 888);
		System.out.println(isa.getAddress().getHostAddress());
		Assert.assertEquals(isa.getAddress().getHostAddress(), "192.168.1.23");
	}
}
