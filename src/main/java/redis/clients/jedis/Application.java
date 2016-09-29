package redis.clients.jedis;

import redis.clients.util.ShardConfigDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lvfei on 16/9/29.
 */
public class Application {


    public static void main(String[] args) {

        List<String> strings = new ArrayList<>();
        strings.add("1|10.33.7.120:30000:2000:master;10.33.7.129:30001:2000:slave;10.33.7.128:30002:2000:slave");
        strings.add("1|10.33.7.121:30000:2000:master;10.33.7.120:30001:2000:slave;10.33.7.129:30002:2000:slave");
        strings.add("1|10.33.7.122:30000:2000:master;10.33.7.121:30001:2000:slave;10.33.7.120:30002:2000:slave");
        strings.add("1|10.33.7.123:30000:2000:master;10.33.7.122:30001:2000:slave;10.33.7.121:30002:2000:slave");
        strings.add("1|10.33.7.124:30000:2000:master;10.33.7.123:30001:2000:slave;10.33.7.122:30002:2000:slave");
        strings.add("1|10.33.7.125:30000:2000:master;10.33.7.124:30001:2000:slave;10.33.7.123:30002:2000:slave");
        strings.add("1|10.33.7.126:30000:2000:master;10.33.7.125:30001:2000:slave;10.33.7.124:30002:2000:slave");
        strings.add("1|10.33.7.127:30000:2000:master;10.33.7.126:30001:2000:slave;10.33.7.125:30002:2000:slave");
        strings.add("1|10.33.7.128:30000:2000:master;10.33.7.127:30001:2000:slave;10.33.7.126:30002:2000:slave");
        strings.add("1|10.33.7.129:30000:2000:master;10.33.7.128:30001:2000:slave;10.33.7.127:30002:2000:slave");

        List<MasterSlaveShardInfo> list = new ArrayList<>();

        for (String string : strings) {
            ShardConfigDecoder decoder = new ShardConfigDecoder(string);
            MasterSlaveShardInfo info = new MasterSlaveShardInfo(decoder.getWeight(), decoder.getMasters(), decoder.getSlaves());
            list.add(info);
        }

        ShardMasterSlaveJedis jedis = new ShardMasterSlaveJedis(list);

//        jedis.set("lvfei", "hello");

        System.out.println(jedis.get("lvfei"));
        jedis.set("lvfei", "test");

        System.out.println(jedis.get("lvfei"));

    }
}
