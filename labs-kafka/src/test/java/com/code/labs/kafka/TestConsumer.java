package com.code.labs.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(TestConsumer.class);

  public static void main(String[] args) {
    String zookeeper = "localhost:2181";
    String group = "group-for-test";
    String topic = "topic-for-test";

    KafkaConsumerGroup kafkaConsumerGroup = new KafkaConsumerGroup(zookeeper, group, topic, 2);
    kafkaConsumerGroup.run(AbstractKafkaConsumer.class);
    LOG.info("Consumer is started!");
  }
}