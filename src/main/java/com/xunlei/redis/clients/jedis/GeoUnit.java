package com.xunlei.redis.clients.jedis;

import com.xunlei.redis.clients.util.SafeEncoder;

public enum GeoUnit {
  M, KM, MI, FT;

  public final byte[] raw;

  GeoUnit() {
    raw = SafeEncoder.encode(this.name().toLowerCase());
  }
}
