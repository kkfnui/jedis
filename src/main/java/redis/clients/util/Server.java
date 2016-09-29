package redis.clients.util;

/**
 * Created by lvfei on 16/9/28.
 */
public class Server {

    private int timeout;
    private String host;
    private int port;
    private String password;

    public Server(String host, int port) {
        this.host = host;
        this.port = port;
        this.timeout = 2000;
    }

    public Server(String host, int port, int timeout, String password) {
        this.timeout = timeout;
        this.host = host;
        this.port = port;
        this.password = password;
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}
