package com.code.labs.curator.queue;

public class ZkPath {

  public final static String NAMESPACE = "labs-curator";
  private final static String QUEUE_ROOT = "/queue";

  public static String queuePrefixPath() {
    return QUEUE_ROOT + "/tasks/task-";
  }

  public static String queuePath() {
    return QUEUE_ROOT + "/tasks";
  }

  public static String taskPath(String taskId) {
    return QUEUE_ROOT + "/tasks/" + taskId;
  }

  public static String lockPath(String taskId) {
    return QUEUE_ROOT + "/lock/" + taskId;
  }
}
