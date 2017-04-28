# redis-example
使用Redis的Java客户端Jedis，封装实现Cache/SortedSet/PubSub/Lock等功能。

### 示例：
* 抽象Redis的操作类型，主要是query和execute，前者有返回结果；

```
	public abstract class RedisAction<T> {
	  public T query(Jedis jedis) { return null; }
	  public void execute(Jedis jedis) {}
	}
```

* RedisTemplate中从JedisPool中获得Jedis对象，执行完query或execute后返回；

```
	public <T> T query(RedisAction<T> action) {
	  Jedis jedis = jedisPool.getResource();
	  try {
      return action.query(jedis);
	  } finally {
      if (jedis != null) {
        jedisPool.returnResource(jedis);
      }
	  }
	}
	public <T> void execute(RedisAction<T> redisTemplateAction) {
	  Jedis jedis = jedisPool.getResource();
	  try {
	    redisTemplateAction.execute(jedis);
	  } finally {
	    if (jedis != null) {
	      jedisPool.returnResource(jedis);
	    }
	  }
	}
```

* Cache/SortedSet/PubSub中根据具体的需求，定义RedisAction，使用RedisTemplate执行具体的逻辑。

```
	public class RedisSet {
	  public void add(final String key, final String member) {
	    redisTemplate.execute(new RedisAction<String>() {
	      public void execute(Jedis jedis) {
	        jedis.sadd(key, member);
	      }
	    });
	  }
	}
```  
### 要点：
1. **Redis**：使用"单线程-多路复用IO模型"来实现高性能的内存数据服务，支持的数据类型包括String，Hash，List，Set，SortedSet等。因为Redis是单线程操作的一块大内存，高IOPS，避免了使用锁，无上下文切换。在同一个多核的服务器中，可以启动多个实例。耗时的O(N)或O(logN)命令，会消耗更多的CPU，影响并发；

2. **Jedis**：Redis的Java客户端实现，对Redis的操作进行了封装。支持Redis的Pipeline和Transaction，Pipeline将一系列请求连续发送给Server端，不等待单个请求的返回，Server端会将这些请求顺序执行(不保证原子执行)完成后，再一次性将其返回值发送回客户端，即Group Commit的意思。Transaction会将事务中的一序列请求按提交顺序原子执行，其中出现语法错误，整个事务会回滚，出现运行错误则还会继续执行后面事务中的其他请求。

3. **分布式锁**：主要会构造一个会过期的键值对，key是锁的名字，value是UUID，往往是是标识目前把持锁的对象。申请锁的时候，执行"SET lockName uuid NX EX 超时时间"成功则获取该锁，即当前没有任何其他对象把持该锁，该超时时间往往超过对象需要把持锁的时间；释放锁的时候，当"GET lockName == uuid"的情况下才"DEL lockName"。这是因为如果把持锁的时间过了超时时间，其他对象会同时把持该锁，更新lockName的值，于是前一个把持锁的对象不能简单进行DEL操作；需要重入锁的时候，判断比较uuid是否等于"GET lockName"。