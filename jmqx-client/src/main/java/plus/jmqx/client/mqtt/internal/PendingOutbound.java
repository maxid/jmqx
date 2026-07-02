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

    private final MqttPublish                  publish;
    private final Sinks.One<MqttPublishResult> resultSink;
    private final long                         sentNanos;

    public PendingOutbound(MqttPublish publish, Sinks.One<MqttPublishResult> resultSink) {
        this.publish = publish;
        this.resultSink = resultSink;
        this.sentNanos = System.nanoTime();
    }

    public MqttPublish getPublish() {
        return publish;
    }

    public Sinks.One<MqttPublishResult> getResultSink() {
        return resultSink;
    }

    public long getSentNanos() {
        return sentNanos;
    }

}
