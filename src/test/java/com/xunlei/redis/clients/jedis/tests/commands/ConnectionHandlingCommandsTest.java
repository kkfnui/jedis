package com.xunlei.redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;

import com.xunlei.redis.clients.jedis.BinaryJedis;
import com.xunlei.redis.clients.jedis.HostAndPort;
import org.junit.Test;

import com.xunlei.redis.clients.jedis.tests.HostAndPortUtil;

public class ConnectionHandlingCommandsTest extends JedisCommandTestBase {
  protected static HostAndPort hnp = HostAndPortUtil.getRedisServers().get(0);

  @Test
  public void quit() {
    assertEquals("OK", jedis.quit());
  }

  @Test
  public void binary_quit() {
    BinaryJedis bj = new BinaryJedis(hnp.getHost());
    assertEquals("OK", bj.quit());
  }
}