package com.code.labs.curator;


import com.code.labs.curator.leaderelection.Master;
import com.code.labs.curator.leaderelection.Worker;
import org.junit.Test;

public class TestLeaderElection {

  private static final String ZK_ADDRESS = "127.0.0.1:2181";

  @Test
  public void runWorker() throws Exception {
    Worker worker = new Worker(ZK_ADDRESS);
    worker.start();
    worker.join();
  }

  @Test
  public void runMaster() throws Exception {
    Master master = new Master(ZK_ADDRESS);
    master.start();
    master.join();
  }
}
