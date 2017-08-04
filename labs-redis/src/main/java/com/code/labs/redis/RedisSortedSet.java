package com.code.labs.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisSortedSet implements Closeable {

  private JedisPool jedisPool;

  private static final String LUA_POP =
      "local elements = redis.call('zrevrange', KEYS[1], 0, tonumber(ARGV[1]));\n" +
      "if next(elements) ~= nil then\n" +
          "redis.call('zrem', KEYS[1], unpack(elements));\n" +
      "end\n" +
      "return elements;";

  public RedisSortedSet(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public void add(final String key, final double weight, final String element) {
    try (Jedis jedis = jedisPool.getResource()) {
      // store in asc order by default
      jedis.zadd(key, weight, element);
    }
  }

  public List<String> pop(final String key, final long size) {
    try (Jedis jedis = jedisPool.getResource()) {
      return (List<String>) jedis.eval(LUA_POP, Arrays.asList(key), Arrays.asList(String.valueOf(size - 1)));
    }
  }

  public long size(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.zcard(key);
    }
  }

  @Override
  public void close() throws IOException {
    if (jedisPool != null) jedisPool.close();
  }
}