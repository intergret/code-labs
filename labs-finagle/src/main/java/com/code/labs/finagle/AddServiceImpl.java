package com.code.labs.finagle;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddServiceImpl implements AddService.ServiceIface {

  private static final Logger LOG = LoggerFactory.getLogger(AddServiceImpl.class);
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
            Thread.sleep(5000);
            LOG.info("Executor info {}", executorService);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    };
    checkupThread.setDaemon(true);
    checkupThread.start();
  }

  @Override
  public Future<Integer> request(final int num1, final int num2) {
    return futurePool.apply(new Function0<Integer>() {
      @Override
      public Integer apply() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.info("The future was cancelled by client!");
          LOG.info("Interrupted exception {}", Throwables.getStackTraceAsString(e));
        }
        return num1 + num2;
      }
    });
  }

  public void close() {
    executorService.shutdown();
  }
}
