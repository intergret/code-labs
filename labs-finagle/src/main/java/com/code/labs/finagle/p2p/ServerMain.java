package com.code.labs.finagle.p2p;

import com.code.labs.finagle.AddServiceImpl;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerMain {

  private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class);
  public static String IP = "127.0.0.1";
  public static String PORT = "9801";

  public static void main(String[] args) {
    String address = IP + ":" + PORT;
    final ListeningServer server = Thrift.server().serveIface(address, new AddServiceImpl());
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        server.close();
        LOG.info("Server stopped!");
      }
    });

    try {
      LOG.info("Server started!");
      Await.ready(server);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
