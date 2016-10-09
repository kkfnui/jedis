package com.xunlei.redis.clients.util;

import com.xunlei.redis.clients.jedis.exceptions.InvalidURIException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lvfei on 16/9/28.
 */
public class ShardConfigDecoder {

    private List<Server> masters = new ArrayList<Server>();
    private List<Server> slavers = new ArrayList<Server>();
    private int weight = 1;

    //config  weight|host:port:timeout:{master/slave};host:port:timeout:{master/slave}
    public ShardConfigDecoder(String config) {
        String[] items = config.split("\\|");
        if (items.length != 2) {
            throw new InvalidURIException("expect one \"|\" in config");
        }
        weight = Integer.parseInt(items[0]);
        parseServers(items[1]);
    }

    private void parseServers(String item) {
        String[] servers = item.split(";");
        for (String server : servers) {
            parseServer(server);
        }
    }

    private void parseServer(String server) {
        String[] values = server.split(":");
        if (values.length != 4) {
            throw new InvalidURIException("there should 4 item in server, but in fact is: " + server);
        }
        String host = values[0];
        Integer port = Integer.parseInt(values[1]);
        Integer timeout = Integer.parseInt(values[2]);
        String ms = values[3];

        Server s = new Server(host, port, timeout, null);
        if (ms.equals("master")) {
            masters.add(s);
        } else if (ms.equals("slave")) {
            slavers.add(s);
        } else {
            throw new InvalidURIException("unknow master/slave option: " + ms);
        }
    }


    public int getWeight() {
        return weight;
    }

    public List<Server> getMasters() {
        return masters;
    }

    public List<Server> getSlaves() {
        return slavers;
    }
}
