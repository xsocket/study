package com.github.xsocket.study.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Created by MWQ on 17/7/7.
 */
public class CuratorTest {

  private static int LOCK_COUNT = 0;

  private static final CuratorFramework CLIENT = CuratorFrameworkFactory.builder()
      .connectString("127.0.0.1:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 1))
      .build();

  //@Test
  public void testCreate() throws Exception {
    CLIENT
        .create()
        .creatingParentsIfNeeded()
        .forPath("/test/curator", "TestCreate".getBytes());

    byte[] bytes = CLIENT.getData().forPath("/test/curator");
    Assert.assertEquals("TestCreate", new String(bytes));
  }

  //@Test
  public void testLock() throws Exception {
    LOCK_COUNT = 0;
    final InterProcessMutex lock = new InterProcessMutex(CLIENT, "/test/curator/lock");
    for(int i = 0; i < 30; i++) {
      final int threadIndex = i;
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            lock.acquire();
            try {
              System.out.println("Thread:" + threadIndex +", count:" + LOCK_COUNT++);
            } finally {
              lock.release();
            }
          } catch(Exception e) {
            e.printStackTrace(System.err);
          }
        }
      }).start();
    }
    Thread.sleep(5000);
  }

  @Test
  public void doNothing() {

  }

  @BeforeClass
  public static void init() {
    CLIENT.start();
  }

  @AfterClass
  public static void destroy() {
    CLIENT.close();
  }
}
