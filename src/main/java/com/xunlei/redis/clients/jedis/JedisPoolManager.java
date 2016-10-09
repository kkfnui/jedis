package com.xunlei.redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import com.xunlei.redis.clients.util.Server;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by lvfei on 16/9/28.
 */
public class JedisPoolManager {


    private ConcurrentMap<String, JedisPool> pools = new ConcurrentHashMap<String, JedisPool>();


    public JedisPool getJedisPool(final Server server) {
        String key = server.getHost() + ":" + server.getPort();
        JedisPool pool = pools.get(key);
        if (pool != null) {
            return pool;
        }

        pool = new JedisPool(new GenericObjectPoolConfig(),
                server.getHost(),
                server.getPort(),
                server.getTimeout(),
                server.getPassword());

        JedisPool prev = pools.putIfAbsent(key, pool);
        if (prev == null) {
            return pool;
        } else {
            pool.close();
            return prev;
        }
    }

    public JedisPool getJedisPool(final Server server, final GenericObjectPoolConfig poolConfig) {
        String key = server.getHost() + ":" + server.getPort();
        JedisPool pool = pools.get(key);
        if (pool != null) {
            return pool;
        }

        pool = new JedisPool(poolConfig,
                server.getHost(),
                server.getPort(),
                server.getTimeout(),
                server.getPassword());

        JedisPool prev = pools.putIfAbsent(key, pool);
        if (prev == null) {
            return pool;
        } else {
            pool.close();
            return prev;
        }
    }


}
