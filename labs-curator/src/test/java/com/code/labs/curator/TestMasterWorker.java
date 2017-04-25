package com.code.labs.curator;


import org.junit.Test;

public class TestMasterWorker {

  private static final String ZK_ADDRESS = "127.0.0.1:2181";

  @Test
  public void testWorker() throws Exception {
    Worker worker = new Worker(ZK_ADDRESS);
    worker.start();
    worker.join();
  }

  @Test
  public void testMaster() throws Exception {
    Master master = new Master(ZK_ADDRESS);
    master.start();
    master.join();
  }
}
