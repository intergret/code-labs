package com.code.labs.kafka;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Author: Xing Wang <wangxing.bjtu@gmail.com>
 * Date: 2017-03-06 Time: 19:47
 */
public class KafkaConsumerGroup<T extends AbstractKafkaConsumer> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerGroup.class);

  private final ConsumerConnector consumer;

  private final String topic;

  private final Integer threadNumber;

  private ExecutorService executor;

  private static final String REBALANCE_BACKOFF_MS = "6000";
  private static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "6000";
  private static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "10000";
  private static final String ZOOKEEPER_SYNC_TIME_MS = "4000";

  public KafkaConsumerGroup(String zookeeperAddress, String groupId, String topic, Integer threadNumber) {
    consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeperAddress, groupId));
    this.topic = topic;
    this.threadNumber = threadNumber;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        shutdown();
      }
    });
  }

  private static ConsumerConfig createConsumerConfig(String zookeeperAddress, String groupId) {
    Properties props = new Properties();
    try {
      props.put("zookeeper.connect", zookeeperAddress);
      props.put("group.id", groupId);
      props.put("zookeeper.session.timeout.ms", ZOOKEEPER_SESSION_TIMEOUT_MS);
      props.put("zookeeper.connection.timeout.ms", ZOOKEEPER_CONNECTION_TIMEOUT_MS);
      props.put("zookeeper.sync.time.ms", ZOOKEEPER_SYNC_TIME_MS);
      props.put("rebalance.backoff.ms", REBALANCE_BACKOFF_MS);
      props.put("consumer.id", "consumer-id-prefix");
      return new ConsumerConfig(props);
    } catch (Exception e) {
      LOG.error("", e);
    }
    return null;
  }

  public void run(Class<T> clazz, Object... args) {
    Map<String,Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, threadNumber);
    Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[],byte[]>> streams = consumerMap.get(topic);

    executor = Executors.newFixedThreadPool(threadNumber);
    Integer threadSeq = 0;
    try {
      List<Class> paramClazz = new ArrayList<>();
      paramClazz.add(KafkaStream.class);
      paramClazz.add(Integer.class);
      List<Object> basicParamValues = new ArrayList<>();
      for (Object arg : args) {
        paramClazz.add(arg.getClass());
        basicParamValues.add(arg);
      }

      Constructor constructor = clazz.getConstructor(paramClazz.toArray(new Class[paramClazz.size()]));
      for (final KafkaStream stream : streams) {
        List<Object> paramValues = new ArrayList<>();
        paramValues.add(stream);
        paramValues.add(threadSeq);
        paramValues.addAll(basicParamValues);
        executor.submit((T) constructor.newInstance(paramValues.toArray()));
        threadSeq++;
      }
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
      LOG.error("", e);
    }
  }

  public void shutdown() {
    if (consumer != null) consumer.shutdown();
    if (executor != null) MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
    LOG.info("Consumer and executor shut down successfully.");
  }
}
