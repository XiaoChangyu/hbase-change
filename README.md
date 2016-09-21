# hbase-change
对 https://github.com/apache/hbase 的小改动. 目前只改了 hbase-client-1.1.2.

对使用 hbase 的开发者来说, 可能有些时候会头疼于 hosts 的配置, 因为 hbase 写入到 zookeeper 的 hbase 集群信息使用的是 hostname, 而不是 ip.
而 hbase 客户端需要连接到这些集群中的服务器, 这样在远程连接 hbase 集群的时候就需要对 集群服务器的 hostname 进行解析.
通常的做法是, 在运行 hbase 客户端的服务器上配置 hostname 映射关系.
但在部分特殊的场景下, 如临时测试, 特殊权限的服务器, 不是很方便去配置 hostname 映射.
这个时候就很想在 hbase 客户端程序中进行配置或设置 hostname 映射关系.
于是, 这个改造 hbase-client 的计划就出来了.

PS:
1, 这个改造是直接拿 hbase-1.1.2 源码进行的, 由于只更改了 hbase-client, 所以把其余的组件都删掉了, 并且将 hbase-client 更名为 hbase-client-change, 以便和原来的 hbase-client 进行区分.
2, hbase-client-change 中有如何设置 hostname 映射关系的例子, 路径是 test.TestSetHostnameIp.
3, hbase-client-change 的 pom.xml 中增加了 maven deploy 到 nexus 上的脚本, 可以方便的使用 hbase-client-change 进行发布.
4, 执行 mvn 命令的时候建议添加 -DskipTests 来跳过 src/test/java 下的测试代码, 因为里面的部分测试代码会报错.
