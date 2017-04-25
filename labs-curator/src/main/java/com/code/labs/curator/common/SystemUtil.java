package com.code.labs.curator.common;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;

public class SystemUtil {

  public static String LOCAL_HOST = "127.0.0.1";
  public static int PORT_MIN = 1025;
  public static int PORT_MAX = 65535;

  public static String getLocalIPAddress() {
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
    } catch (SocketException e) {}
    throw new IllegalStateException("Couldn't find the local ip address!");
  }

  public static int getAvailablePort() {
    for (int port = PORT_MIN; port < PORT_MAX; port++) {
      try {
        Socket socket = new Socket(LOCAL_HOST, port);
        socket.close();
        return port;
      } catch (IOException e) {}
    }
    return -1;
  }
}
