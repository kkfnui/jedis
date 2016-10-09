package com.xunlei.redis.clients.jedis.tests;

import static org.junit.Assert.assertEquals;

import com.xunlei.redis.clients.jedis.BuilderFactory;
import org.junit.Test;

public class BuilderFactoryTest {
  @Test
  public void buildDouble() {
    Double build = BuilderFactory.DOUBLE.build("1.0".getBytes());
    assertEquals(new Double(1.0), build);
  }
}