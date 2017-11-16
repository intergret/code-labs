package com.code.labs.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Author: Xing Wang <wangxing.bjtu@gmail.com>
 * Date: 2017-03-06 Time: 19:49
 */
public class KafkaProducer {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);

  private Producer producer;
  private String topic;
  private String brokerList;

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public void setBrokerList(String brokerList) {
    this.brokerList = brokerList;
  }

  public KafkaProducer(String brokerList, String topic) {
    this.brokerList = brokerList;
    this.topic = topic;
    Properties props = new Properties();
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", brokerList);
    producer = new Producer<>(new ProducerConfig(props));
  }

  public void send(String message) {
    try {
      producer.send(new KeyedMessage<>(topic, DigestUtils.md5Hex(message), message));
    } catch (Exception e) {
      LOG.error("{}", Throwables.getStackTraceAsString(e));
    }
  }

  public void batchSend(List<String> messages) {
    List<KeyedMessage> keyedMessages = new ArrayList<>();
    for (String message : messages) {
      keyedMessages.add(new KeyedMessage(topic, DigestUtils.md5Hex(message), message));
    }
    producer.send(keyedMessages);
  }
}
