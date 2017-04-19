package com.code.labs.finagle.cluster.server;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.code.labs.finagle.AddServiceImpl;
import com.twitter.finagle.Announcement;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Future;

public class ServerMain {

  private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class);
  public static int PORT = 9801;
  public static String ZK = "127.0.0.1:2181";
  public static String ZK_PATH = "/nodes";

  private AddServiceImpl addService;
  private ListeningServer listeningServer;
  private Future<Announcement> clusterStatus;

  public void startServer() {
    try {
      addService = new AddServiceImpl();
      listeningServer = Thrift.serveIface(new InetSocketAddress(PORT), addService);
      ServerAnnouncer zkAnnouncer = new ServerAnnouncer();
      clusterStatus = zkAnnouncer.announce(ZK, ZK_PATH, PORT);
      System.out.println("Server start on zk:" + ZK + ", path:" + ZK_PATH + ", port:" + PORT);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          close();
        }
      });
      Await.ready(listeningServer);
    } catch (Exception e) {
      System.out.println(e);
      LOG.error("Start listeningServer failed : {}", Throwables.getStackTraceAsString(e));
      close();
    }
  }

  public void close() {
    if (clusterStatus != null) {
      try {
        Await.result(clusterStatus).unannounce();
      } catch (Exception e) {
        LOG.error("{}", Throwables.getStackTraceAsString(e));
      }
    }
    if (addService != null) {
      addService.close();
    }
    if (listeningServer != null) {
      listeningServer.close();
    }
    LOG.info("Server shutdown.");
  }

  public static void main(String[] args) {
    ServerMain server = null;
    try {
      server = new ServerMain();
      server.startServer();
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Server start failed : {}", Throwables.getStackTraceAsString(e));
      if (server != null) {
        server.close();
      }
    }
  }
}