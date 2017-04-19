package com.code.labs.finagle.cluster.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.github.zkclient.IZkChildListener;

public abstract class ZkEventListener implements IZkChildListener {

  private final Set<String> preNodes = new HashSet<>();

  public ZkEventListener(String path, List<String> children) {
    if (children != null) {
      preNodes.addAll(children);
      onChildChange(path, children, children, new ArrayList<String>());
    }
  }

  @Override
  public synchronized void handleChildChange(String parent, List<String> children) throws Exception {
    if (children == null) {
      return;
    }

    Set<String> previousNodes = new HashSet<>(preNodes);
    Set<String> currentNodes = new HashSet<>(children);

    // deleted. previous - new nodes
    previousNodes.removeAll(currentNodes);

    // new added. new nodes - previous nodes
    currentNodes.removeAll(preNodes);

    preNodes.addAll(currentNodes);
    preNodes.removeAll(previousNodes);
    List<String> added = new ArrayList<>(currentNodes);
    List<String> deleted = new ArrayList<>(previousNodes);
    onChildChange(parent, children, added, deleted);
  }

  public abstract void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted);
}
