package com.flink.domain;

public class Access {
    public String device;
    public String deviceType;
    public String os;
    public String event;
    public String net;
    public String channel;
    public String uid;
    public int nu;  //1新
    public String ip;
    public long time;
    public String version;
    public Product product;

    @Override
    public String toString() {
        return "Access{" +
                "device='" + device + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", os='" + os + '\'' +
                ", event='" + event + '\'' +
                ", net='" + net + '\'' +
                ", channel='" + channel + '\'' +
                ", uid='" + uid + '\'' +
                ", nu=" + nu +
                ", ip='" + ip + '\'' +
                ", time=" + time +
                ", version='" + version + '\'' +
                ", product=" + product +
                '}';
    }
}
