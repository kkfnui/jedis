package com.xunlei.redis.clients.jedis;

import com.xunlei.redis.clients.jedis.commands.JedisCommands;
import com.xunlei.redis.clients.jedis.exceptions.JedisException;
import com.xunlei.redis.clients.jedis.params.geo.GeoRadiusParam;
import com.xunlei.redis.clients.jedis.params.set.SetParams;
import com.xunlei.redis.clients.jedis.params.sortedset.ZAddParams;
import com.xunlei.redis.clients.jedis.params.sortedset.ZIncrByParams;
import com.xunlei.redis.clients.util.Hashing;
import com.xunlei.redis.clients.util.Sharded;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by lvfei on 16/9/28.
 */
public class ShardMasterSlaveJedis extends Sharded<ShardServers, MasterSlaveShardInfo> implements JedisCommands {

    private JedisPoolManager poolManager = new JedisPoolManager();

    public ShardMasterSlaveJedis(List<MasterSlaveShardInfo> shards) {
        super(shards, Hashing.MURMUR_HASH, Sharded.DEFAULT_KEY_TAG_PATTERN);
    }


    private void test() {


        //shard.close();
    }

    @Override
    public String set(String key, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.set(key, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String set(String key, String value, SetParams params) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.set(key, value, params);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String get(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.get(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Boolean exists(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.exists(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long persist(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.persist(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String type(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.type(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long expire(String key, int seconds) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.expire(key, seconds);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.pexpire(key, milliseconds);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.expireAt(key, unixTime);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.pexpireAt(key, millisecondsTimestamp);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long ttl(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.ttl(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long pttl(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.pttl(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setbit(key, offset, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setbit(key, offset, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Boolean getbit(String key, long offset) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.getbit(key, offset);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setrange(key, offset, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.getrange(key, startOffset, endOffset);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String getSet(String key, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.getSet(key, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long setnx(String key, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setnx(key, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String setex(String key, int seconds, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.setex(key, seconds, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String psetex(String key, long milliseconds, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.psetex(key, milliseconds, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long decrBy(String key, long integer) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.decrBy(key, integer);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long decr(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.decr(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long incrBy(String key, long integer) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.incrBy(key, integer);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Double incrByFloat(String key, double value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.incrByFloat(key, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long incr(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.incr(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long append(String key, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.append(key, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String substr(String key, int start, int end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.substr(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long hset(String key, String field, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hset(key, field, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String hget(String key, String field) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hget(key, field);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hsetnx(key, field, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hmset(key, hash);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hmget(key, fields);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hincrBy(key, field, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Double hincrByFloat(String key, String field, double value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hincrByFloat(key, field, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Boolean hexists(String key, String field) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hexists(key, field);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long hdel(String key, String... field) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hdel(key, field);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long hlen(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hlen(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> hkeys(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hkeys(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<String> hvals(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hvals(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hgetAll(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long rpush(String key, String... string) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.rpush(key, string);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long lpush(String key, String... string) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lpush(key, string);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long llen(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.llen(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lrange(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String ltrim(String key, long start, long end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.ltrim(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String lindex(String key, long index) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lindex(key, index);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String lset(String key, long index, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lset(key, index, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long lrem(String key, long count, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lrem(key, count, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String lpop(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lpop(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String rpop(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.rpop(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long sadd(String key, String... member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sadd(key, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> smembers(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.smembers(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long srem(String key, String... member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.srem(key, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String spop(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.spop(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> spop(String key, long count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.spop(key, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long scard(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.scard(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Boolean sismember(String key, String member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sismember(key, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String srandmember(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.srandmember(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<String> srandmember(String key, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.srandmember(key, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long strlen(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.strlen(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zadd(String key, double score, String member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zadd(key, score, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zadd(key, score, member, params);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zadd(key, scoreMembers);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zadd(key, scoreMembers, params);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrange(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zrem(String key, String... member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrem(key, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zincrby(key, score, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Double zincrby(String key, double score, String member, ZIncrByParams params) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zincrby(key, score, member, params);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zrank(String key, String member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrank(key, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zrevrank(String key, String member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrank(key, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrange(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeWithScores(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeWithScores(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zcard(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zcard(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Double zscore(String key, String member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zscore(key, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<String> sort(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sort(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sort(key, sortingParameters);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zcount(String key, double min, double max) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zcount(key, min, max);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zcount(String key, String min, String max) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zcount(key, min, max);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScore(key, min, max);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScore(key, min, max);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScore(key, max, min);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScore(key, min, max, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScore(key, max, min);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScore(key, min, max, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScore(key, max, min, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScoreWithScores(key, min, max);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScoreWithScores(key, max, min);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScore(key, max, min, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScoreWithScores(key, min, max);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScoreWithScores(key, max, min);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zremrangeByRank(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zremrangeByScore(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zremrangeByScore(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zlexcount(key, min, max);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByLex(key, min, max);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrangeByLex(key, min, max, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByLex(key, max, min);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrangeByLex(key, max, min, offset, count);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zremrangeByLex(key, min, max);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long linsert(String key, BinaryClient.LIST_POSITION where, String pivot, String value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.linsert(key, where, pivot, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long lpushx(String key, String... string) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.lpushx(key, string);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long rpushx(String key, String... string) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.rpushx(key, string);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.blpop(timeout, key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.brpop(timeout, key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long del(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.del(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String echo(String string) {
        ShardServers servers = getShard(string);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.echo(string);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long move(String key, int dbIndex) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.move(key, dbIndex);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long bitcount(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.bitcount(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.bitcount(key, start, end);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long bitpos(String key, boolean value) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.bitpos(key, value);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long bitpos(String key, boolean value, BitPosParams params) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.bitpos(key, value, params);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hscan(key, cursor);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.hscan(key, cursor, params);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sscan(key, cursor);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zscan(key, cursor);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zscan(key, cursor, params);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor, ScanParams params) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.sscan(key, cursor, params);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long pfadd(String key, String... elements) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.pfadd(key, elements);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public long pfcount(String key) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.pfcount(key);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long geoadd(String key, double longitude, double latitude, String member) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.geoadd(key, longitude, latitude, member);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.geoadd(key, memberCoordinateMap);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Double geodist(String key, String member1, String member2) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.geodist(key, member1, member2);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Double geodist(String key, String member1, String member2, GeoUnit unit) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.geodist(key, member1, member2, unit);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<String> geohash(String key, String... members) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.geohash(key, members);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<GeoCoordinate> geopos(String key, String... members) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.geopos(key, members);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.georadius(key, longitude, latitude, radius, unit);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.georadius(key, longitude, latitude, radius, unit, param);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.georadiusByMember(key, member, radius, unit);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, false);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.georadiusByMember(key, member, radius, unit, param);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public List<Long> bitfield(String key, String... arguments) {
        ShardServers servers = getShard(key);
        JedisPool jedisPool = servers.getPool(poolManager, true);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.bitfield(key, arguments);
        } catch (JedisException e) {
            jedisPool.returnBrokenResource(jedis);
            jedis = null;
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }
}
