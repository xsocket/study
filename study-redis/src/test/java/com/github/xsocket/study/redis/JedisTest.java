package com.github.xsocket.study.redis;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

/**
 * Jedis测试用例
 * Created by MWQ on 16/11/6.
 */
public class JedisTest {

  @Test
  public void testConnection() {
    this.runTestCase(new RedisTestCase() {
      @Override
      public void doTest(Jedis jedis) {
        Assert.assertEquals("PONG", jedis.ping());
      }
    });
  }

  @Test
  public void testBasicOperation() {
    this.runTestCase(new RedisTestCase() {
      @Override
      public void doTest(Jedis jedis) {
        // key - value
        Assert.assertEquals("OK", jedis.set("testKey", "testValue"));
        Assert.assertEquals("testValue", jedis.get("testKey"));
        
        // list
        jedis.del("animals");
        String[] list = new String[] { "fox", "bull", "dragon" };
        Assert.assertEquals((long)list.length, (long)jedis.rpush("animals", list));
        Assert.assertArrayEquals(list, jedis.lrange("animals", 0, list.length - 1).toArray());
      }
    });
  }

  private void runTestCase(RedisTestCase test) {
    Jedis jedis = new Jedis("weikuai.aliyun");
    test.doTest(jedis);
    jedis.close();
  }

  private interface RedisTestCase {
    void doTest(Jedis jedis);
  }
}
