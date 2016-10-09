package com.xunlei.redis.clients.wapper;

import com.xunlei.redis.clients.jedis.ShardMasterSlaveJedis;

/**
 * Created by lvfei on 16/10/9.
 */
public class RedisTable {
    private static String COLON = ":";
    private String table;
    private ShardMasterSlaveJedis client;

    public RedisTable(String table, ShardMasterSlaveJedis jedis) {
        this.table = table;
        this.client = jedis;
    }

    public static String getKey(String table, String key) {
        if (table == null || table.isEmpty())
            return key;

        return table + COLON + key;
    }

    private String transformKey(String key) {
        return getKey(table, key);
    }

    
}
