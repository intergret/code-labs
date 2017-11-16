package com.code.labs.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * Author: Xing Wang <wangxing.bjtu@gmail.com>
 * Date: 2017-03-06 Time: 19:34
 */
public class AbstractKafkaConsumer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKafkaConsumer.class);

  protected KafkaStream kafkaStream;

  protected Integer threadSeq;

  public AbstractKafkaConsumer(KafkaStream kafkaStream, Integer threadSeq) {
    this.kafkaStream = kafkaStream;
    this.threadSeq = threadSeq;
  }

  public void run() {
    long threadId = Thread.currentThread().getId();
    ConsumerIterator<byte[],byte[]> it = kafkaStream.iterator();
    while (it.hasNext()) {
      MessageAndMetadata<byte[],byte[]> next = it.next();
      System.out.println("threadId:" + threadId + " partition:" + next.partition() + " offset:" + next.offset()
          + " message:" + new String(next.message()));
    }
    LOG.info("Shutting down Thread: " + threadSeq);
  }
}