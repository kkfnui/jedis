package com.xunlei.redis.clients.jedis;

import com.xunlei.redis.clients.jedis.exceptions.JedisServerException;
import com.xunlei.redis.clients.util.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by lvfei on 16/9/28.
 */
public class ShardServers {

    private List<Server> masters = new ArrayList<Server>();
    private List<Server> slaves = new ArrayList<Server>();

    public ShardServers() {

    }

    public void addMasters(List<Server> servers) {
        masters.addAll(servers);
    }

    public void addSlaves(List<Server> servers) {
        slaves.addAll(servers);
    }

    public JedisPool getPool(JedisPoolManager poolManager, boolean isWrite) {
        if (slaves.isEmpty() && masters.isEmpty()) {
            throw new JedisServerException("no server on shard");
        }

        if (isWrite && masters.isEmpty()) {
            throw new JedisServerException("no master server when executor a write command");
        }

        if (!isWrite && slaves.isEmpty()) {
            throw new JedisServerException("no slave server when executor a read command");
        }

        Server server;
        Random random = new Random();
        if (isWrite) {
            int i = random.nextInt(masters.size());
            server = masters.get(i);
        } else {
            int i = random.nextInt(slaves.size());
            server = slaves.get(i);
        }

        return poolManager.getJedisPool(server);
    }
}
