package com.xunlei.redis.clients.wapper;

import com.xunlei.redis.clients.jedis.ShardMasterSlaveJedis;
import com.xunlei.redis.clients.jedis.Tuple;

import java.util.*;

import static com.xunlei.redis.clients.jedis.params.set.SetParams.setParams;

/**
 * Created by lvfei on 16/10/9.
 */
public class JedisWapperSsdbTable {
    private static String COLON = ":";
    private String table;
    private ShardMasterSlaveJedis client;


    public JedisWapperSsdbTable(String table, ShardMasterSlaveJedis jedis) {
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

    //// 兼容 ssdb 命令

    public String getValue(String key) {
        String strKey = transformKey(key);
        return client.get(strKey);
    }

    public boolean setValue(String key, String value) {
        String strKey = transformKey(key);
        client.set(strKey, value);
        return true;
    }


    public String hget(String key, String field) {
        String strKey = transformKey(key);
        return client.hget(strKey, field);
    }


    public boolean existsKey(String key) {
        String strKey = transformKey(key);
        return client.exists(strKey);
    }

    public void setx(String key, String value, int ttl) {
        String strKey = transformKey(key);
        client.set(strKey, value, setParams().ex(ttl));
    }

    public void del(String key) {
        String strKey = transformKey(key);
        client.del(strKey);
    }


    public boolean zset(String name, String key, long score) {
        String strName = transformKey(name);
        client.zadd(strName, score, key);
        return true;
    }

    public Long zsize(String name) {
        String strName = transformKey(name);
        return client.zcard(strName);
    }

//    public void multiSet(List<KeyValue> keyValues) {
//        for (KeyValue keyValue : keyValues) {
//            keyValue.setKey(transformKey(keyValue.getKey()));
//        }
//        client.multiSet(keyValues);
//    }
//
//    public List<KeyValue> multiGet(List<String> keys) {
//        List<String> newKeys = new ArrayList<String>(keys.size());
//        for (String key : keys) {
//            key = transformKey(key);
//            newKeys.add(key);
//        }
//        List<KeyValue> keyValues = client.multiGet(newKeys);
//        for (KeyValue keyValue : keyValues) {
//            String key = keyValue.getKey();
//            String[] rawKeys = key.split(":");
//            if (rawKeys.length != 2) {
//                continue;
//            }
//            String rawKey = rawKeys[1];
//            keyValue.setKey(rawKey);
//        }
//        return keyValues;
//
//    }

    public boolean multiZset(String name, List<IdScore> idScores) {
        if (idScores.isEmpty()) {
            return false;
        }
        String strName = transformKey(name);
        Map<String, Double> maps = new HashMap<>();
        for (IdScore idScore : idScores) {
            maps.put(idScore.getId(), (double) idScore.getScore());
        }
        client.zadd(strName, maps);
        return true;
    }

    public void multiZdel(String name, List<String> idScores) {
        String strName = transformKey(name);
        client.zrem(strName, idScores.toArray(new String[idScores.size()]));
    }

    public List<IdScore> zrrange1(String name, int offset, int limit) {
        String strName = transformKey(name);
        Set<Tuple> tuples = client.zrevrangeWithScores(strName, offset, offset + limit - 1);
        List<IdScore> idScores = new ArrayList<>(tuples.size());
        for (Tuple tuple : tuples) {
            long score = Math.round(tuple.getScore());
            IdScore idScore = new IdScore(tuple.getElement(), score);
            idScores.add(idScore);
        }
        return idScores;
    }


    public List<IdScore> zrange(String key, int offset, int limit) {
        String strName = transformKey(key);
        Set<Tuple> tuples = client.zrangeWithScores(strName, offset, offset + limit - 1);

        List<IdScore> idScores = new ArrayList<>(tuples.size());
        for (Tuple tuple : tuples) {
            long score = Math.round(tuple.getScore());
            IdScore idScore = new IdScore(tuple.getElement(), score);
            idScores.add(idScore);
        }
        return idScores;
    }

    //// 兼容 ssdb 命令
}
