package com.code.labs.finagle.cluster.server;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.twitter.finagle.Announcement;
import com.twitter.finagle.zookeeper.ZkAnnouncer;
import com.twitter.finagle.zookeeper.ZkClientFactory;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import scala.Option;

public class ServerAnnouncer {

  private ZkAnnouncer zkAnnouncer;

  public ServerAnnouncer() {
    ZkClientFactory zkClientFactory = new ZkClientFactory(Duration.apply(30, TimeUnit.SECONDS));
    zkAnnouncer = new ZkAnnouncer(zkClientFactory);
  }

  public static String getHostAddress() {
    try {
      for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
        if (!iface.getName().startsWith("vmnet") && !iface.getName().startsWith("docker")) {
          for (InetAddress raddr : Collections.list(iface.getInetAddresses())) {
            if (raddr.isSiteLocalAddress() && !raddr.isLoopbackAddress() && !(raddr instanceof Inet6Address)) {
              return raddr.getHostAddress();
            }
          }
        }
      }
    } catch (SocketException e) {
    }
    throw new IllegalStateException("Couldn't find the local machine ip.");
  }

  public Future<Announcement> announce(String zk, String path, int port) {
    // set socket address to 127.0.0.1 for local debug
    // new InetSocketAddress(getHostAddress(), port)
    return zkAnnouncer.announce(zk, path, 0, new InetSocketAddress("127.0.0.1", port), Option.<String> empty());
  }
}
