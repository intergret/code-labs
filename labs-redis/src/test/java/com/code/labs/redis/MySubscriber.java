package com.code.labs.redis;

import redis.clients.jedis.JedisPubSub;

public class MySubscriber extends JedisPubSub {

  @Override
  public void onSubscribe(String channel, int subscribedChannels) {
    System.out.println("Subscribe turn on");
  }

  @Override
  public void onPSubscribe(String pattern, int subscribedChannels) {
    System.out.println("Pattern subscribe turn on");
  }

  @Override
  public void onMessage(String channel, String message) {
    System.out.println("Receive message with subscribe:" + channel + " " + message);
  }

  @Override
  public void onPMessage(String pattern, String channel, String message) {
    System.out.println("Receive message with pattern subscribe:" + channel + " " + message);
  }

  @Override
  public void onUnsubscribe(String channel, int subscribedChannels) {
    System.out.println("Subscribe turn off");
  }

  @Override
  public void onPUnsubscribe(String pattern, int subscribedChannels) {
    System.out.println("Pattern subscribe turn off");
  }
}
