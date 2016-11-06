package com.github.xsocket.study.kafka;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.fluent.Request;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 猫眼票房数据生产者
 * Created by MWQ on 16/10/29.
 */
public class KafkaProducer implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  private static final String API_URL_DAILY_BOXOFFICE = "http://piaofang.maoyan.com/history/daily/box.json?date=%s";

  @Override
  public void run() {
    Date now = new Date();
    String url = String.format(API_URL_DAILY_BOXOFFICE, DATE_FORMAT.format(now));
    try {
      String content = Request.Get(url).execute().returnContent().asString();
      LOGGER.debug("content: {}", content);
      ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaUtils.TOPIC, now.getTime(), content);
      Producer<Long, String> producer = KafkaUtils.newProducer();
      producer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata meta, Exception e) {
          if(e != null) {
            LOGGER.warn("Error occured when sending data", e);
          } else {
            LOGGER.debug("Send result: offset={}, key={}", meta.offset(), now.getTime());
          }
        }
      });
      producer.close();
      LOGGER.debug("send: {}", content);
    } catch (IOException e) {
      String msg = String.format("Fail to fetch maoyan daily boxoffice, %s", url);
      LOGGER.warn(msg, e);
    }
  }

  public static void main(String[] args) {
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    service.scheduleAtFixedRate(new KafkaProducer(), 0, 10, TimeUnit.MINUTES);
  }
}
