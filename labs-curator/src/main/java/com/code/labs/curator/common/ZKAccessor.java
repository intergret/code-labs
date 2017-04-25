package com.code.labs.curator.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class ZKAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(ZKAccessor.class);

  private static CuratorFramework client;

  public ZKAccessor(String zkAddress, String namespace) {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    client = CuratorFrameworkFactory.builder()
        .namespace(namespace)
        .connectString(zkAddress)
        .retryPolicy(retryPolicy)
        .connectionTimeoutMs(3000)
        .sessionTimeoutMs(3000)
        .build();
    client.start();
  }

  public CuratorFramework getClient() {
    return client;
  }

  public String create(CreateMode mode, final String zkPath, final String payload) {
    try {
      return client.create().creatingParentsIfNeeded()
          .withMode(mode)
          .forPath(zkPath, payload.getBytes());
    } catch (Exception e) {
      logError(e);
    }
    return null;
  }

  public void setData(final String zkPath, final String payload) {
    try {
      client.setData().forPath(zkPath, payload.getBytes());
    } catch (Exception e) {
      logError(e);
    }
  }

  public String getData(final String zkPath) {
    try {
      if (client.checkExists().forPath(zkPath) != null) {
        byte[] data = client.getData().forPath(zkPath);
        if (data != null) return new String(data);
      }
    } catch (Exception e) {
      logError(e);
    }
    return null;
  }

  public void delete(final String zkPath) {
    try {
      client.delete().guaranteed().deletingChildrenIfNeeded().forPath(zkPath);
    } catch (Exception e) {
      logError(e);
    }
  }

  public List<String> getChildren(final String zkPath, boolean fullPath) {
    try {
      if (client.checkExists().forPath(zkPath) != null) {
        List<String> childrenPath = new ArrayList<>();
        List<String> paths = client.getChildren().forPath(zkPath);
        if (paths != null) {
          Collections.sort(paths);
          if (fullPath) {
            for (String childPath : paths) {
              childrenPath.add(ZKPaths.makePath(zkPath, childPath));
            }
          } else {
            childrenPath.addAll(paths);
          }
        }
        return childrenPath;
      }
    } catch (Exception e) {
      logError(e);
    }
    return null;
  }

  public void close() {
    CloseableUtils.closeQuietly(client);
  }

  private void logError(Exception e) {
    LOG.error("{}", Throwables.getStackTraceAsString(e));
  }
}
