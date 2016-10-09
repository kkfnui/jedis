package com.xunlei.redis.clients.jedis;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.xunlei.redis.clients.util.SafeEncoder;

public class ScanParams {

  private final Map<Protocol.Keyword, ByteBuffer> params = new EnumMap<Protocol.Keyword, ByteBuffer>(Protocol.Keyword.class);

  public final static String SCAN_POINTER_START = String.valueOf(0);
  public final static byte[] SCAN_POINTER_START_BINARY = SafeEncoder.encode(SCAN_POINTER_START);

  public ScanParams match(final byte[] pattern) {
    params.put(Protocol.Keyword.MATCH, ByteBuffer.wrap(pattern));
    return this;
  }

  public ScanParams match(final String pattern) {
    params.put(Protocol.Keyword.MATCH, ByteBuffer.wrap(SafeEncoder.encode(pattern)));
    return this;
  }

  public ScanParams count(final Integer count) {
    params.put(Protocol.Keyword.COUNT, ByteBuffer.wrap(Protocol.toByteArray(count)));
    return this;
  }

  public Collection<byte[]> getParams() {
    List<byte[]> paramsList = new ArrayList<byte[]>(params.size());
    for (Map.Entry<Protocol.Keyword, ByteBuffer> param : params.entrySet()) {
      paramsList.add(param.getKey().raw);
      paramsList.add(param.getValue().array());
    }
    return Collections.unmodifiableCollection(paramsList);
  }

  String match() {
    if (params.containsKey(Protocol.Keyword.MATCH)) {
      return new String(params.get(Protocol.Keyword.MATCH).array());
    } else {
      return null;
    }
  }

  Integer count() {
    if (params.containsKey(Protocol.Keyword.COUNT)) {
      return params.get(Protocol.Keyword.COUNT).getInt();
    } else {
      return null;
    }
  }
}
