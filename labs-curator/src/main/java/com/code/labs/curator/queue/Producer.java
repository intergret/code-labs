package com.code.labs.curator.queue;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.code.labs.curator.common.ZKAccessor;
import com.google.common.base.Throwables;

public class Producer extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
  private ZKAccessor zkAccessor;

  public Producer(String zkAddress) {
    this.zkAccessor = new ZKAccessor(zkAddress, ZkPath.NAMESPACE);
  }

  @Override
  public void run() {
    while (true) {
      try {
        doProduce();
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOG.error("Producer interrupted exception {}", Throwables.getStackTraceAsString(e));
      }
    }
  }

  public void doProduce() {
    String taskPath = zkAccessor.create(CreateMode.EPHEMERAL_SEQUENTIAL, ZkPath.queuePrefixPath(), "Task");
    LOG.info("Producer new task to {}", taskPath);
  }
}
