package com.xunlei.redis.clients.jedis.exceptions;

public class JedisServerException extends RuntimeException {
  private static final long serialVersionUID = -2946266495682282677L;

  public JedisServerException(String message) {
    super(message);
  }

  public JedisServerException(Throwable e) {
    super(e);
  }

  public JedisServerException(String message, Throwable cause) {
    super(message, cause);
  }
}
