package com.code.labs.curator;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.code.labs.curator.common.SystemUtil;
import com.code.labs.curator.common.ZKAccessor;
import com.code.labs.curator.common.ZkPathUtil;
import com.google.common.base.Throwables;

public class Master extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(Master.class);
  private volatile boolean runAsLeader = false;
  private volatile boolean runAsNormal = false;

  private ZKAccessor zkAccessor;

  private String masterId;
  private String ipAddress;
  private int port;

  private TreeCache prevMasterWatcher;
  private PathChildrenCache workerWatcher;

  public Master(String zkAddress) {
    this.zkAccessor = new ZKAccessor(zkAddress, ZkPathUtil.NAMESPACE);
    this.ipAddress = SystemUtil.getLocalIPAddress();
    this.port = SystemUtil.getAvailablePort();

    String payload = ZkPathUtil.createPayload(this.ipAddress, this.port);
    String fullPath = zkAccessor.create(CreateMode.EPHEMERAL_SEQUENTIAL, ZkPathUtil.allMasterPrefixPath(), payload);
    this.masterId = ZKPaths.getNodeFromPath(fullPath);
  }

  @Override
  public void run() {
    competeLeader();
  }

  private void competeLeader() {
    List<String> allMaster = zkAccessor.getChildren(ZkPathUtil.allMasterPath(), false);
    String oldestMaster = allMaster.get(0);
    if (masterId.endsWith(oldestMaster)) {
      runAsLeader();
    } else {
      String prevMasterId = allMaster.get(allMaster.indexOf(masterId) - 1);
      runAsNormal(prevMasterId);
    }
  }

  public void runAsLeader() {
    runAsLeader = true;

    String payload = ZkPathUtil.createPayload(ipAddress, port);
    zkAccessor.create(CreateMode.EPHEMERAL, ZkPathUtil.leaderMasterPath(masterId), payload);

    // watch worker
    try {
      workerWatcher = new PathChildrenCache(zkAccessor.getClient(), ZkPathUtil.workerPath(), false);
      workerWatcher.start(PathChildrenCache.StartMode.NORMAL);
      workerWatcher.getListenable().addListener(new PathChildrenCacheListener() {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
          LOG.info("Leader master watch event {} on worker!", event);
          switch (event.getType()) {
            case CHILD_ADDED:
              String addedWorkerId = ZKPaths.getNodeFromPath(event.getData().getPath());
              LOG.info("Leader master {} find new worker join {}", masterId, addedWorkerId);
              break;
            case CHILD_REMOVED:
              String removedWorkerId = ZKPaths.getNodeFromPath(event.getData().getPath());
              LOG.info("Leader master {} find old worker left {}", masterId, removedWorkerId);
              break;
          }
        }
      });
    } catch (Exception e) {
      LOG.error("Watch worker exception {}", Throwables.getStackTraceAsString(e));
    }

    LOG.info("Leader master {} start.", masterId);
    while (runAsLeader) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        LOG.error("Leader master exception {}", Throwables.getStackTraceAsString(e));
      }
    }

    quitLeader();
    LOG.info("Leader master {} quit.", masterId);
    competeLeader();
  }

  public void runAsNormal(String prevMasterId) {
    runAsNormal = true;

    // watch master
    try {
      final String prevMasterPath = ZkPathUtil.masterPath(prevMasterId);
      prevMasterWatcher = new TreeCache(zkAccessor.getClient(), prevMasterPath);
      prevMasterWatcher.start();
      prevMasterWatcher.getListenable().addListener(new TreeCacheListener() {
        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
          LOG.info("Normal master watch event {} on prev master {}!", event, prevMasterPath);
          switch (event.getType()) {
            case NODE_REMOVED:
              if (prevMasterPath.equals(event.getData().getPath())) {
                LOG.info("Normal master {} find prev master {} left!", masterId, prevMasterPath);
                runAsNormal = false;
              }
              break;
          }
        }
      });
      LOG.info("Normal master {} watch on prev master {}", masterId, prevMasterId);
    } catch (Exception e) {
      LOG.error("Watch prev master exception {}", Throwables.getStackTraceAsString(e));
    }

    LOG.info("Normal master {} start.", masterId);
    while (runAsNormal) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        LOG.error("Normal master exception {}", Throwables.getStackTraceAsString(e));
      }
    }

    quitNormal();
    LOG.info("Normal master {} quit.", masterId);
    competeLeader();
  }

  private void quitLeader() {
    if (prevMasterWatcher != null) {
      CloseableUtils.closeQuietly(prevMasterWatcher);
    }
  }

  private void quitNormal() {
    if (workerWatcher != null) {
      workerWatcher.getListenable().clear();
      CloseableUtils.closeQuietly(workerWatcher);
    }
  }
}
