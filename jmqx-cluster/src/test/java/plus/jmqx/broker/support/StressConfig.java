package plus.jmqx.broker.support;

/**
 * MQTT 消息压测参数
 *
 * @author maxid
 * @since 2026/6/27
 */
public class StressConfig {

    public int    port;
    public int    threads;
    public int    durationSeconds;
    public int    payloadBytes;
    public int    flushEvery;
    public int    inFlightLimit;
    public int    timeoutSeconds;
    public int    reportIntervalSeconds;
    public String topic;

}
