package com.code.labs.ehcache;

import java.util.Collection;
import java.util.List;

import net.sf.ehcache.Statistics;

public interface CacheService {

  List<String> listCacheNames();

  boolean exist(String cacheName);

  boolean exist(String cacheName, String cacheKey);

  <T> void put(String cacheName, String cacheKey, T cacheItem);

  <T> T get(String cacheName, String cacheKey);

  List getKeys(String cacheName);

  void remove(String cacheName, String cacheKey);

  void remove(String cacheName, Collection<String> cacheKeys);

  void remove(String cacheName);

  Statistics getStatistics(String cacheName);

}
