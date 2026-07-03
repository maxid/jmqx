package plus.jmqx.client.mqtt.stress;

import plus.jmqx.client.mqtt.message.QoS;

/**
 * jmqx-client 压测参数（可通过 {@code -Djmqx.client.stress.*} 覆盖）。
 */
public class ClientStressConfig {

    public String brokerHost;
    public int    brokerPort;
    public int    messages;
    public int    threads;
    public int    subscribers;
    public int    publishers;
    public int    connections;
    public int    payloadBytes;
    public QoS    qos;
    public int    minThroughputMsgPerSec;
    public int    inflight;
    public int    timeoutSeconds;
    public int    progressIntervalSeconds;
    public String topic;

}
