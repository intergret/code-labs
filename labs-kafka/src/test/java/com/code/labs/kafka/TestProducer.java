package com.code.labs.kafka;

public class TestProducer {

  public static void main(String[] args) throws InterruptedException {
    String brokerList = "localhost:9092";
    String topic = "topic-for-test";
    KafkaProducer kafkaProducer = new KafkaProducer(brokerList, topic);

    for (int a = 0; a < 20; a++) {
      kafkaProducer.send("this is a message" + a);
      Thread.sleep(100);
    }

    Thread.sleep(10 * 1000);
    System.exit(0);
  }
}
