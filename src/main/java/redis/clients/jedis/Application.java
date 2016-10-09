package redis.clients.jedis;

import redis.clients.util.ShardConfigDecoder;

import java.util.ArrayList;
import java.util.List;

import static redis.clients.jedis.params.set.SetParams.setParams;

/**
 * Created by lvfei on 16/9/29.
 */
public class Application {


    public static void main(String[] args) {

        List<String> strings = new ArrayList<>();
        strings.add("1|10.33.7.115:30000:2000:master;10.33.7.129:30001:2000:slave;10.33.7.128:30002:2000:slave");
        strings.add("1|10.33.7.116:30000:2000:master;10.33.7.115:30001:2000:slave;10.33.7.129:30002:2000:slave");
        strings.add("1|10.33.7.117:30000:2000:master;10.33.7.116:30001:2000:slave;10.33.7.115:30002:2000:slave");
        strings.add("1|10.33.7.118:30000:2000:master;10.33.7.117:30001:2000:slave;10.33.7.116:30002:2000:slave");
        strings.add("1|10.33.7.119:30000:2000:master;10.33.7.118:30001:2000:slave;10.33.7.117:30002:2000:slave");
        strings.add("1|10.33.7.120:30000:2000:master;10.33.7.119:30001:2000:slave;10.33.7.118:30002:2000:slave");
        strings.add("1|10.33.7.121:30000:2000:master;10.33.7.120:30001:2000:slave;10.33.7.119:30002:2000:slave");
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

        final int count = 1000000;

        for (int i = 0; i < count; i++) {
            String key = Integer.toString(i);
            String value = key;
            String ret = jedis.set(key, value, setParams().ex(3600));
            if (i % 10000 == 0) {
                System.out.print(".");
            }
        }

        for (int i = 0; i < count; i++) {
            String key = Integer.toString(i);
            String s = jedis.get(key);
            if (!key.equals(s)) {
                System.out.println("not equle, expected: " + key + ", real: " + s);
            }
        }

    }
}
