package com.code.labs.curator;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import com.code.labs.curator.common.SystemUtil;
import com.code.labs.curator.common.ZKAccessor;
import com.code.labs.curator.common.ZkPathUtil;

public class Master extends Thread {

  private volatile boolean runAsLeader = false;
  private volatile boolean runAsNormal = false;

  private ZKAccessor zkAccessor;

  private String masterId;
  private String ipAddress;
  private int port;

  private PathChildrenCache leaderWatcher;
  private PathChildrenCacheListener leaderListener;
  private PathChildrenCache workerWatcher;
  private PathChildrenCacheListener workerListener;

  public Master(String zkAddress) {
    this.zkAccessor = new ZKAccessor(zkAddress, ZkPathUtil.NAMESPACE);
    this.ipAddress = SystemUtil.getLocalIPAddress();
    this.port = SystemUtil.getAvailablePort();

    String payload = ZkPathUtil.createPayload(this.ipAddress, this.port);
    String zkPathPre = ZkPathUtil.allMasterPath() + "/master-";
    String fullPath = zkAccessor.create(CreateMode.EPHEMERAL_SEQUENTIAL, zkPathPre, payload);
    this.masterId = ZKPaths.getNodeFromPath(fullPath);

    this.leaderListener = new PathChildrenCacheListener() {
      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
          case CHILD_ADDED:
            String addedWorkerId = ZKPaths.getNodeFromPath(event.getData().getPath());
            System.out.println("leader master " + masterId + " find new worker join " + addedWorkerId);
            break;
          case CHILD_REMOVED:
            String removedWorkerId = ZKPaths.getNodeFromPath(event.getData().getPath());
            System.out.println("leader master " + masterId + " find old worker left " + removedWorkerId);
            break;
        }
      }
    };

    this.workerListener = new PathChildrenCacheListener() {
      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
          case CHILD_ADDED:
            String addedMasterId = ZKPaths.getNodeFromPath(event.getData().getPath());
            System.out.println("normal master " + masterId + " find leader master " + addedMasterId);
            break;
          case CHILD_REMOVED:
            String removedMasterId = ZKPaths.getNodeFromPath(event.getData().getPath());
            System.out.println("normal master " + masterId + " find leader master left " + removedMasterId);
            runAsNormal = false;
            break;
        }
      }
    };
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
      runAsNormal();
    }
  }

  public void runAsLeader() {
    runAsLeader = true;

    String payload = ZkPathUtil.createPayload(ipAddress, port);
    String leaderPath = ZkPathUtil.leaderMasterPath() + "/" + masterId;
    zkAccessor.create(CreateMode.EPHEMERAL, leaderPath, payload);

    // watch worker
    try {
      workerWatcher = new PathChildrenCache(zkAccessor.getClient(), ZkPathUtil.workerPath(), false);
      workerWatcher.start(PathChildrenCache.StartMode.NORMAL);
      workerWatcher.getListenable().addListener(leaderListener);
    } catch (Exception e) {
      System.out.println("watch worker exception " + e);
    }

    System.out.println("leader master " + masterId + " start.");
    while (runAsLeader) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        System.out.println(e);
      }
    }

    quitLeader();
    System.out.println("leader master " + masterId + " quit.");
    competeLeader();
  }

  public void runAsNormal() {
    runAsNormal = true;

    // watch master
    try {
      leaderWatcher = new PathChildrenCache(zkAccessor.getClient(), ZkPathUtil.leaderMasterPath(), false);
      leaderWatcher.start(PathChildrenCache.StartMode.NORMAL);
      leaderWatcher.getListenable().addListener(workerListener);
    } catch (Exception e) {
      System.out.println("watch leader master exception " + e);
    }

    System.out.println("normal master " + masterId + " start.");
    while (runAsNormal) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        System.out.println(e);
      }
    }

    quitNormal();
    System.out.println("normal master " + masterId + " quit.");
    competeLeader();
  }

  private void quitLeader() {
    if (leaderWatcher != null) {
      leaderWatcher.getListenable().clear();
      CloseableUtils.closeQuietly(leaderWatcher);
    }
  }

  private void quitNormal() {
    if (workerWatcher != null) {
      workerWatcher.getListenable().clear();
      CloseableUtils.closeQuietly(workerWatcher);
    }
  }
}
