package com.code.labs.ehcache;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.Statistics;

public class EhcacheService implements CacheService {

  private static final Logger LOG = LoggerFactory.getLogger(EhcacheService.class);

  private CacheManager cacheManager;

  public EhcacheService(CacheManager cacheManager) {
    this.cacheManager = cacheManager;
  }

  @Override
  public List<String> listCacheNames() {
    return Arrays.asList(cacheManager.getCacheNames());
  }

  @Override
  public boolean exist(String cacheName) {
    return getCache(cacheName) != null;
  }

  @Override
  public boolean exist(String cacheName, String cacheKey) {
    Cache cache = getCache(cacheName);
    if (cache == null) {
      return false;
    }
    return cache.get(cacheKey) != null;
  }

  @Override
  public <T> void put(String cacheName, String cacheKey, T cacheItem) {
    Cache cache = getCache(cacheName);
    if (cache != null) {
      cache.put(new Element(cacheKey, cacheItem));
    }
  }

  @Override
  public <T> T get(String cacheName, String cacheKey) {
    Cache cache = getCache(cacheName);
    if (cache == null) {
      return null;
    }

    Element element = cache.get(cacheKey);
    if (element != null) {
      try {
        return (T) element.getValue();
      } catch (Exception e) {
        LOG.error("", e);
      }
    }
    return null;
  }

  @Override
  public List getKeys(String cacheName) {
    Cache cache = getCache(cacheName);
    if (cache != null) {
      return cache.getKeys();
    }
    return Collections.EMPTY_LIST;
  }

  @Override
  public void remove(String cacheName, Collection<String> cacheKeys) {
    Cache cache = getCache(cacheName);
    if (cache != null) {
      cache.removeAll(cacheKeys);
    }
  }

  @Override
  public void remove(String cacheName, String cacheKey) {
    Cache cache = getCache(cacheName);
    if (cache != null) {
      cache.remove(cacheKey);
    }
  }

  @Override
  public void remove(String cacheName) {
    Cache cache = getCache(cacheName);
    if (cache != null) {
      cache.removeAll();
    }
  }

  @Override
  public Statistics getStatistics(String cacheName) {
    Cache cache = getCache(cacheName);
    if (cache != null) {
      return cache.getStatistics();
    }
    return null;
  }

  private Cache getCache(String cacheName) {
    return cacheManager.getCache(cacheName);
  }
}
