# Code-Labs
1. **[Curator](labs-curator/readme.md)**：使用Zookeeper实现类似Leader Election、Queue等功能;

2. **[Finagle](labs-finagle/readme.md)**：基于Finagle搭建RPC服务，包括使用Zookeeper注册服务的Client-Server模式和点对点的Peer2Peer模式;

3. **[Redis](labs-redis/readme.md)**：使用Redis作Counter、Cache、Queue、Set时的一些封装，也基于Redis实现了Lock、PubSub等功。使用了Java版本的客户端Jedis;

4. **Ehcache**：类似Guava的Local Cache，进行简单的封装;