package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.MqttMessageBuilder;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * 不可变的 {@link Mqtt3Publish} 实现。
 *
 * @author maxid
 */
public final class Mqtt3PublishImpl implements Mqtt3Publish {

    private final String  topic;
    private final byte[]  payload;
    private final QoS     qos;
    private final boolean retain;
    private final boolean dup;
    private final int     packetId;

    public Mqtt3PublishImpl(String topic, byte[] payload, QoS qos, boolean retain, boolean dup, int packetId) {
        this.topic = topic;
        this.payload = MqttMessageBuilder.cloneBytes(payload);
        this.qos = qos;
        this.retain = retain;
        this.dup = dup;
        this.packetId = packetId;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public byte[] getPayloadAsBytes() {
        return payload.clone();
    }

    @Override
    public QoS getQoS() {
        return qos;
    }

    @Override
    public boolean isRetain() {
        return retain;
    }

    @Override
    public boolean isDup() {
        return dup;
    }

    @Override
    public int getPacketId() {
        return packetId;
    }

    @Override
    public Mqtt3PublishBuilder toBuilder() {
        return new Mqtt3PublishBuilder()
                .topic(topic).payload(payload).qos(qos)
                .retain(retain).dup(dup).packetId(packetId);
    }

}
