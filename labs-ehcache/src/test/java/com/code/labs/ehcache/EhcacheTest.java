package com.code.labs.ehcache;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import net.sf.ehcache.CacheManager;

public class EhcacheTest {

  private CacheService cacheService;

  private String CACHE_USER = "users";

  @Before
  public void prepare() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("ehcache.xml").getFile());
    CacheManager cacheManager = CacheManager.newInstance(file.getAbsolutePath());
    cacheService = new EhcacheService(cacheManager);
  }

  @Test
  public void testCache() throws Exception {
    System.out.println(cacheService.getStatistics(CACHE_USER));
    cacheService.put(CACHE_USER, "user1", new User(1L, "user1", 10));
    cacheService.get(CACHE_USER, "user1");
    cacheService.get(CACHE_USER, "user2");
    cacheService.put(CACHE_USER, "user2", new User(2L, "user2", 20));
    cacheService.get(CACHE_USER, "user2");
    System.out.println(cacheService.getStatistics(CACHE_USER));
  }
}