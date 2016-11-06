package com.github.xsocket.study.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

/**
 * Created by MWQ on 16/10/29.
 */
public class KafkaUtils {

  public static final String TOPIC = "maoyan";

  public static Producer<Long, String> newProducer() {
    Properties props = new Properties();
    //props.put("zookeeper.connect", "zookeeper.master:2181");//声明zk
    props.put("bootstrap.servers", "kafka.broker1:9092,kafka.broker2:9092,kafka.broker3:9092");
    props.put("retries", new Integer(3));
    //props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return new KafkaProducer<Long, String>(props);
  }

  public static Consumer<Long, String> newConsumer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "kafka.broker1:9092,kafka.broker2:9092,kafka.broker3:9092");
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    //props.put("max.poll.interval.ms", "100000");
    props.put("auto.commit.interval.ms", "1000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    return new KafkaConsumer<Long, String>(props);
  }
}
