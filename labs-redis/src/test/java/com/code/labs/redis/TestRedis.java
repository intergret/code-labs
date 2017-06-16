package com.code.labs.redis;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestRedis {

  private final static String KEY = "testkey";
  private final static String VALUE = "testvalue";
  private final static String CHANNEL = "testchannel";
  private final static String PCHANNEL = "*testchannel*";
  private final static String LOCK = "testlock";

  private JedisPool jedisPool;
  private RedisCache redisCache;
  private RedisCounter redisCounter;
  private RedisPubSub redisPubSub;
  private RedisLock redisLock;
  private RedisQueue redisQueue;
  private RedisSet redisSet;
  private RedisSortedSet redisSortedSet;
  private RedisHashMap redisHashMap;

  @Before
  public void prepare() {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxTotal(3);
    config.setMaxIdle(1);
    config.setMaxWaitMillis(1000);
    config.setTestOnBorrow(true);

    jedisPool = new JedisPool(config, "localhost");
    redisCache = new RedisCache(jedisPool);
    redisCounter = new RedisCounter(jedisPool);
    redisPubSub = new RedisPubSub(jedisPool);
    redisLock = new RedisLock(jedisPool);
    redisQueue = new RedisQueue(jedisPool);
    redisSet = new RedisSet(jedisPool);
    redisSortedSet = new RedisSortedSet(jedisPool);
    redisHashMap = new RedisHashMap(jedisPool);
  }

  @Test
  public void testCache() throws Exception {
    redisCache.set(KEY, VALUE);
    Assert.assertEquals(VALUE, redisCache.get(KEY));
    redisCache.delete(KEY);
    Assert.assertEquals(false, redisCache.exist(KEY));

    Map<String,String> keyValues = new HashMap<>();
    keyValues.put(KEY, VALUE);
    redisCache.batchSet(keyValues);
    Assert.assertEquals(VALUE, redisCache.batchGet(Arrays.asList(KEY)).get(KEY));
    redisCache.batchDelete(Arrays.asList(KEY));
    Assert.assertEquals(false, redisCache.exist(KEY));

    redisCache.set(KEY, VALUE, 1);
    Thread.sleep(1000);
    Assert.assertEquals(false, redisCache.exist(KEY));

    redisCache.batchSet(keyValues, 1);
    Thread.sleep(1000);
    Assert.assertEquals(false, redisCache.exist(KEY));
  }

  @Test
  public void testCounter() throws Exception {
    redisCounter.set(KEY, 0);
    Assert.assertEquals(true, 0 == redisCounter.get(KEY));
    redisCounter.delete(KEY);
    redisCounter.incre(KEY, 1);
    Assert.assertEquals(true, 1 == redisCounter.get(KEY));
    redisCounter.set(KEY, -1) ;
    Assert.assertEquals(true, -1 == redisCounter.get(KEY));

    Map<String,Long> keyIncres = new HashMap<>();
    keyIncres.put(KEY, 1L);
    redisCounter.batchIncre(keyIncres);
    Assert.assertEquals(true, 0 == redisCounter.batchGet(Arrays.asList(KEY)).get(KEY));
    redisCounter.batchSet(keyIncres);
    Assert.assertEquals(true, 1 == redisCounter.batchGet(Arrays.asList(KEY)).get(KEY));
    redisCounter.batchDelete(Arrays.asList(KEY));
    Assert.assertEquals(false, redisCounter.exist(KEY));
  }

  @Test
  public void testPubSub() throws Exception {
    final CountDownLatch countDownLatch = new CountDownLatch(2);
    Thread subscriber = new Thread() {
      @Override
      public void run() {
        countDownLatch.countDown();
        redisPubSub.subscribe(CHANNEL, new MySubscriber());
      }
    };
    subscriber.start();

    Thread psubscriber = new Thread() {
      @Override
      public void run() {
        countDownLatch.countDown();
        redisPubSub.psubscribe(PCHANNEL, new MySubscriber());
      }
    };
    psubscriber.start();

    countDownLatch.await();
    Thread.sleep(3000);
    redisPubSub.publish(CHANNEL, "msg1");
    redisPubSub.batchPublish(CHANNEL, Arrays.asList("msg2"));
    Thread.sleep(3000);
  }

  @Test
  public void testLock() throws Exception {
    int threadNum = 4;
    ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
    for (int i = threadNum; i > 0; i--) {
      executorService.submit(new Runnable() {
        public void run() {
          while (true) {
            long threadId = Thread.currentThread().getId();
            long current = System.currentTimeMillis();
            System.out.println(current + " Thread " + threadId + " ACQUIRE!");
            boolean success = redisLock.acquire(LOCK, 3000);
            current = System.currentTimeMillis();
            if (success) {
              System.out.println(current + " Thread " + threadId + " GET!");
              try {
                Thread.sleep(1000);
              } catch (Exception e) {}
              redisLock.release(LOCK);
              current = System.currentTimeMillis();
              System.out.println(current + " Thread " + threadId + " RELEASE!");
              try {
                Thread.sleep(1000);
              } catch (Exception e) {}
            } else {
              System.out.println(current + " Thread " + threadId + " MISS!");
            }
          }
        }
      });
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(25, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      System.out.println(e);
    }
  }

  @Test
  public void testQueue() throws Exception {
    redisCache.delete(KEY);
    Assert.assertEquals(true, 0 == redisQueue.batchPop(KEY, 3).size());
    redisQueue.push(KEY, VALUE);
    Assert.assertEquals(true, 1 == redisQueue.size(KEY));
    redisQueue.batchPush(KEY, Arrays.asList(VALUE));
    Assert.assertEquals(true, 2 == redisQueue.size(KEY));
    redisQueue.pop(KEY);
    Assert.assertEquals(true, 1 == redisQueue.size(KEY));
    Assert.assertEquals(true, 1 == redisQueue.batchPop(KEY, 3).size());
    Assert.assertEquals(true, 0 == redisQueue.size(KEY));
    redisCache.delete(KEY);
  }

  @Test
  public void testSet() throws Exception {
    redisCache.delete(KEY);
    redisSet.add(KEY, VALUE);
    Assert.assertEquals(true, redisSet.exist(KEY, VALUE));
    redisSet.remove(KEY, VALUE);
    Assert.assertEquals(true, 0 == redisSet.size(KEY));
    redisSet.batchAdd(KEY, Arrays.asList(VALUE));
    Set<String> members = redisSet.list(KEY);
    Assert.assertEquals(true, 1 == members.size() && members.contains(VALUE));
    redisSet.batchRemove(KEY, Arrays.asList(VALUE));
    Assert.assertEquals(true, 0 == redisSet.size(KEY));
    redisCache.delete(KEY);
  }

  @Test
  public void testSortedSet() throws Exception {
    redisCache.delete(KEY);
    redisSortedSet.add(KEY, 2, "a");
    redisSortedSet.add(KEY, 1, "b");
    redisSortedSet.add(KEY, 3, "c");
    Assert.assertEquals(true, 3 == redisSortedSet.size(KEY));
    List<String> elements = redisSortedSet.pop(KEY, 5);
    Assert.assertEquals(true, 3 == elements.size());
    Assert.assertEquals(true, "c".equals(elements.get(0)));
    Assert.assertEquals(true, "a".equals(elements.get(1)));
    Assert.assertEquals(true, "b".equals(elements.get(2)));
    redisCache.delete(KEY);
  }

  @Test
  public void testHashMap() throws Exception {
    redisCache.delete(KEY);
    redisHashMap.set(KEY, "a", "value1");
    redisHashMap.batchSet(KEY, new HashMap<String, String>() {{ put("b","value2"); }});
    Assert.assertEquals(true, 2 == redisHashMap.size(KEY));
    Assert.assertEquals(true, redisHashMap.exists(KEY, "a") && redisHashMap.exists(KEY, "b"));
    Assert.assertEquals(true, "value1".equals(redisHashMap.get(KEY, "a")));
    Map<String,String> fieldValues = redisHashMap.getAll(KEY);
    Assert.assertEquals(true, fieldValues.containsKey("a") && "value1".equals(fieldValues.get("a")));
    Assert.assertEquals(true, fieldValues.containsKey("b") && "value2".equals(fieldValues.get("b")));
    redisHashMap.delete(KEY, "a");
    redisHashMap.batchDelete(KEY, Arrays.asList("b"));
    Assert.assertEquals(true, 0 == redisHashMap.size(KEY));
    redisCache.delete(KEY);
  }

  @After
  public void tearDown() throws IOException {
    if (jedisPool != null) {
      jedisPool.close();
    }
  }
}
