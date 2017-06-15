package com.code.labs.curator.queue;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.code.labs.curator.common.ZKAccessor;
import com.google.common.base.Throwables;

public class Consumer extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
  private ZKAccessor zkAccessor;

  public Consumer(String zkAddress) {
    this.zkAccessor = new ZKAccessor(zkAddress, ZkPath.NAMESPACE);
  }

  @Override
  public void run() {
    while (true) {
      try {
        doConsume();
        Thread.sleep(15);
      } catch (InterruptedException e) {
        LOG.error("Consumer interrupted exception {}", Throwables.getStackTraceAsString(e));
      }
    }
  }

  public void doConsume() {
    List<String> taskIds = zkAccessor.getChildren(ZkPath.queuePath(), false);
    if (taskIds != null && taskIds.size() > 0) {
      for (String taskId : taskIds) {
        LOG.info("Find a task at {}", taskId);

        String lockPath = zkAccessor.create(CreateMode.EPHEMERAL, ZkPath.lockPath(taskId), "");
        if (lockPath != null) {
          LOG.info("Get the lock {}!", lockPath);

          String taskPath = ZkPath.taskPath(taskId);
          if (zkAccessor.exist(taskPath)) {
            zkAccessor.delete(taskPath);
            LOG.info("Consume a task at {} successfully!", taskId);
          } else {
            LOG.info("The task at {} is consumed by other consumer.", taskId);
          }

          zkAccessor.delete(lockPath);
          LOG.info("Release the lock {}!", lockPath);
        } else {
          LOG.info("The task at {} is consuming by other consumer.", taskId);
        }
      }
    } else {
      LOG.info("No task find at {}", ZkPath.queuePath());
    }
  }
}
