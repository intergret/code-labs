package com.code.labs.finagle.p2p;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.code.labs.finagle.AddService;
import com.google.common.base.Throwables;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ClientMain {

  private static final Logger LOG = LoggerFactory.getLogger(ClientMain.class);
  public static String IP = "127.0.0.1";
  public static String PORT = "9801";

  public static void main(String[] args) throws InterruptedException {
    String serverAddress = IP + ":" + PORT;
    final AddService.ServiceIface client = Thrift.client().newIface(serverAddress, AddService.ServiceIface.class);

    int threadNum = 1;
    ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
    for (int i = threadNum; i > 0; i--) {
      executorService.submit(new Runnable() {
        public void run() {
          long threadId = Thread.currentThread().getId();
          int num1 = (int) (1 + Math.random() * 10);
          int num2 = (int) (1 + Math.random() * 10);
          long requestTime = System.currentTimeMillis();
          Future<Integer> res = client.request(num1, num2);
          long futureTime = System.currentTimeMillis();

          try {
            Await.result(res, Duration.apply(500, TimeUnit.MILLISECONDS));
            long finishTime = System.currentTimeMillis();
            LOG.info("ThreadId:{} request:{} future:{} result:{}", threadId, requestTime, futureTime, finishTime);
          } catch (Exception e) {
            if (res != null) {
              res.raise(new Exception(e.toString()));
              // res.cancel();
            }
            LOG.error("Exception {}", e.toString());
          }
        }
      });

      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        LOG.info(Throwables.getStackTraceAsString(e));
      }
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.info(Throwables.getStackTraceAsString(e));
    }
  }
}
