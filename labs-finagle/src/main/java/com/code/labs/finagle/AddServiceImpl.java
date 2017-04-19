package com.code.labs.finagle;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function0;
import com.twitter.util.Future;

public class AddServiceImpl implements AddService.ServiceIface {

  ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("server-pool-thread%d").build();
  ThreadPoolExecutor executorService = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(1000), threadFactory, new ThreadPoolExecutor.DiscardOldestPolicy());
  ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService, true);

  public AddServiceImpl() {
    Thread checkupThread = new Thread() {
      @Override
      public void run() {
        while (true) {
          try {
            Thread.sleep(500);
            System.out.println(executorService);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    };
    checkupThread.start();
  }

  @Override
  public Future<Integer> request(final int num1, final int num2) {
    return futurePool.apply(new Function0<Integer>() {
      @Override
      public Integer apply() {
        try {
          Thread.sleep(num1 + num2);
        } catch (InterruptedException e) {
          System.out.println("The future was cancelled by client!");
        }
        return num1 + num2;
      }
    });
  }

  public void close() {
    executorService.shutdown();
  }
}
