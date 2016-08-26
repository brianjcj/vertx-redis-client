package io.vertx.redis.impl;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/8/26.
 */
public class SentinelList {

    static public class SentinelInfo {
        private String host;
        private int port;

        public SentinelInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }

    private List<SentinelInfo> sentinels = new ArrayList<>();

    public boolean add(SentinelInfo info) {
        // sentinels.remove(info);
        return sentinels.add(info);
    }

    public boolean addToFront(SentinelInfo info) {
        sentinels.remove(info);
        sentinels.add(0, info);
        return true;
    }

    public SentinelInfo get(int index) {
        return sentinels.get(index);
    }

    public int size() {
        return sentinels.size();
    }

    public boolean moveSentinelToFront(int index) {
        SentinelInfo info = sentinels.get(index);
        sentinels.add(0, info);
        return true;
    }

}
