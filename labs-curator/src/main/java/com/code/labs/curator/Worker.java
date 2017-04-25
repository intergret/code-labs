package com.code.labs.curator;

import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import com.code.labs.curator.common.SystemUtil;
import com.code.labs.curator.common.ZKAccessor;
import com.code.labs.curator.common.ZkPathUtil;

public class Worker extends Thread {

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
    System.out.println("worker " + workerId + " start.");
    while (runnable) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        System.out.println(e);
      }
    }
    System.out.println("worker " + workerId + " quit.");
  }

  public void quit() {
    if (zkAccessor != null) zkAccessor.close();
    runnable = false;
  }
}
