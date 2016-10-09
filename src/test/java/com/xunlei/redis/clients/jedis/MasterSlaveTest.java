package com.xunlei.redis.clients.jedis;

import org.junit.Test;
import com.xunlei.redis.clients.util.ShardConfigDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lvfei on 16/9/29.
 */
public class MasterSlaveTest {

    @Test
    public void masterTest() {


        ShardConfigDecoder decoder = new ShardConfigDecoder("1|192.168.99.100:32768:2000:master");

        MasterSlaveShardInfo info = new MasterSlaveShardInfo(decoder.getWeight(), decoder.getMasters(), decoder.getSlaves());
        List<MasterSlaveShardInfo> list = new ArrayList<>();
        list.add(info);
        ShardMasterSlaveJedis jedis = new ShardMasterSlaveJedis(list);

        jedis.set("lvfei", "hello");
    }
}
