package com.xunlei.redis.clients.jedis;

import com.xunlei.redis.clients.util.Server;
import com.xunlei.redis.clients.util.ShardInfo;

import java.util.List;

/**
 * Created by lvfei on 16/9/28.
 */
public class MasterSlaveShardInfo extends ShardInfo<ShardServers> {
    private final List<Server> masters;
    private final List<Server> slaves;


    public MasterSlaveShardInfo(int weight, List<Server> masters, List<Server> slaves) {
        super(weight);
        this.masters = masters;
        this.slaves = slaves;
    }

    @Override
    protected ShardServers createResource() {

        ShardServers servers = new ShardServers();
        servers.addMasters(masters);
        servers.addSlaves(slaves);
        return servers;
    }

    @Override
    public String getName() {
        return null;
    }
}
