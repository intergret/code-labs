package com.code.labs.curator;

import com.google.common.base.Throwables;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import com.code.labs.curator.common.SystemUtil;
import com.code.labs.curator.common.ZKAccessor;
import com.code.labs.curator.common.ZkPathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private volatile boolean runnable = true;

  private ZKAccessor zkAccessor;
  private String workerId;
  private String ipAddress;

  private int port;

  public Worker(String zkAddress) {
    this.zkAccessor = new ZKAccessor(zkAddress, ZkPathUtil.NAMESPACE);
    this.ipAddress = SystemUtil.getLocalIPAddress();
    this.port = SystemUtil.getAvailablePort();

    String payload = ZkPathUtil.createPayload(this.ipAddress, this.port);
    String zkPathPre = ZkPathUtil.workerPath() + "/worker-";
    String fullPath = zkAccessor.create(CreateMode.EPHEMERAL_SEQUENTIAL, zkPathPre, payload);
    this.workerId = ZKPaths.getNodeFromPath(fullPath);
  }

  @Override
  public void run() {
    LOG.info("Waster {} start.", workerId);
    while (runnable) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        LOG.info("Waster exception {}", Throwables.getStackTraceAsString(e));
      }
    }
    LOG.info("Waster {} quit.", workerId);
  }

  public void quit() {
    if (zkAccessor != null) zkAccessor.close();
    runnable = false;
  }
}
