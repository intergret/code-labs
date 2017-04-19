package com.code.labs.finagle.p2p;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.code.labs.finagle.AddService;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ClientMain {

  public static String IP = "127.0.0.1";
  public static String PORT = "9801";

  public static void main(String[] args) throws InterruptedException {
    String serverAddress = IP + ":" + PORT;
    final AddService.ServiceIface client = Thrift.client().newIface(serverAddress, AddService.ServiceIface.class);

    int threadNum = 6;
    ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
    for (int i = threadNum; i > 0; i--) {
      executorService.submit(new Runnable() {
        public void run() {
          long threadId = Thread.currentThread().getId();
          while (true) {
            int num1 = (int) (1 + Math.random() * 10);
            int num2 = (int) (1 + Math.random() * 10);
            long requestTime = System.currentTimeMillis();
            Future<Integer> res = client.request(num1, num2);
            long futureTime = System.currentTimeMillis();
            // res.cancel();
            // res.raise(new Exception("The future was cancelled!"));

            try {
              Await.result(res, Duration.apply(1, TimeUnit.SECONDS));
              long finishTime = System.currentTimeMillis();

              System.out.println("[Info] threadId:" + threadId
                  + " request:" + requestTime + ", "
                  + " future:" + futureTime + ", "
                  + " result:" + finishTime);
            } catch (Exception e) {
              System.out.println(threadId + " " + e);
            }
          }
        }
      });

      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        System.out.println(e);
      }
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      System.out.println(e);
    }
  }
}
