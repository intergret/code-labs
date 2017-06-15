package com.code.labs.curator.leaderelection;

public class ZkPath {

  public final static String NAMESPACE = "labs-curator";
  private final static String MASTER_ROOT = "/leader-election/master";
  private final static String WORKER_ROOT = "/leader-election/worker";

  public static String allMasterPath() {
    return MASTER_ROOT + "/all";
  }

  public static String allMasterPrefixPath() {
    return MASTER_ROOT + "/all/master-";
  }

  public static String masterPath(String masterId) {
    return MASTER_ROOT + "/all/" + masterId;
  }

  public static String leaderMasterPath(String masterId) {
    return MASTER_ROOT + "/leader" + "/" + masterId;
  }

  public static String workerPrefixPath() {
    return WORKER_ROOT + "/worker-";
  }

  public static String workerRootPath() {
    return WORKER_ROOT;
  }

  public static String createPayload(String ip, int port) {
    return String.format("{\"ip\":%s,\"port\":%d}", ip, port);
  }
}
