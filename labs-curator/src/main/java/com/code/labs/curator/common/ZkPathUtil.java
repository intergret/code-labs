package com.code.labs.curator.common;

public class ZkPathUtil {

  public final static String NAMESPACE = "test";
  private final static String MASTER_ROOT = "/master";
  private final static String WORKER_ROOT = "/worker";

  public static String allMasterPath() {
    return MASTER_ROOT + "/all";
  }

  public static String leaderMasterPath() {
    return MASTER_ROOT + "/leader";
  }

  public static String workerPath() {
    return WORKER_ROOT;
  }

  public static String createPayload(String ip, int port) {
    return String.format("{\"ip\":%s,\"port\":%d}", ip, port);
  }
}
