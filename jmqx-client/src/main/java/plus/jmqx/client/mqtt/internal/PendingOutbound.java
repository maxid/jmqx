package plus.jmqx.client.mqtt.internal;

import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttPublishResult;
import reactor.core.publisher.Sinks;

/**
 * 一个等待 ACK 的出站 QoS1/2 消息。
 *
 * @author maxid
 */
public final class PendingOutbound {

    /**
     * 发布的 MQTT 消息
     */
    private final MqttPublish                  publish;
    /**
     * 结果发射器
     */
    private final Sinks.One<MqttPublishResult> resultSink;
    /**
     * 发送时间戳（纳秒）
     */
    private final long                         sentNanos;

    /**
     * 构造 PendingOutbound。
     *
     * @param publish    发布的 MQTT 消息
     * @param resultSink 结果发射器
     */
    public PendingOutbound(MqttPublish publish, Sinks.One<MqttPublishResult> resultSink) {
        this.publish = publish;
        this.resultSink = resultSink;
        this.sentNanos = System.nanoTime();
    }

    /**
     * 获取发布的 MQTT 消息。
     *
     * @return 发布消息
     */
    public MqttPublish getPublish() {
        return publish;
    }

    /**
     * 获取结果发射器。
     *
     * @return 结果发射器
     */
    public Sinks.One<MqttPublishResult> getResultSink() {
        return resultSink;
    }

    /**
     * 获取发送时间戳（纳秒）。
     *
     * @return 发送时的 System.nanoTime()
     */
    public long getSentNanos() {
        return sentNanos;
    }

}
