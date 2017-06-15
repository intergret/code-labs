package com.code.labs.curator;

import org.junit.Test;

import com.code.labs.curator.queue.Consumer;
import com.code.labs.curator.queue.Producer;

public class TestQueue {

  private static final String ZK_ADDRESS = "127.0.0.1:2181";

  @Test
  public void runProducer() throws Exception {
    Producer producer = new Producer(ZK_ADDRESS);
    producer.start();
    producer.join();
  }

  @Test
  public void runConsumer() throws Exception {
    Consumer consumer = new Consumer(ZK_ADDRESS);
    consumer.start();
    consumer.join();
  }
}
