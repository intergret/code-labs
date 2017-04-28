package com.code.labs.redis;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class TestLua {

  private Jedis jedis;

  @Before
  public void prepare() {
    jedis = new Jedis("localhost", 6379);
  }

  @Test
  public void testEqualDelete() throws Exception {
    List<String> keys = Arrays.asList("testkey");
    List<String> args = Arrays.asList("testvalue");
    System.out.println((long) jedis.eval(RedisLock.LUA_DELETE, keys, args));
  }

  @Test
  public void testNXEX() throws Exception {
    List<String> keys = Arrays.asList("testkey");
    List<String> args = Arrays.asList("testvalue", "10");
    System.out.println((long) jedis.eval(RedisLock.LUA_NXEX, keys, args));
  }

  @After
  public void close() {
    jedis.close();
  }
}
