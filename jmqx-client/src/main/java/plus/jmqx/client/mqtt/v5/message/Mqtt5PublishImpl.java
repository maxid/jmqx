package plus.jmqx.client.mqtt.v5.message;

import plus.jmqx.client.mqtt.message.MqttMessageBuilder;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * {@link Mqtt5Publish} 的不可变实现。
 *
 * @author maxid
 */
public final class Mqtt5PublishImpl implements Mqtt5Publish {

    private final String topic;
    private final byte[] payload;
    private final QoS qos;
    private final boolean retain;
    private final boolean dup;
    private final int packetId;
    private final Mqtt5PublishProperties properties;

    public Mqtt5PublishImpl(String topic, byte[] payload, QoS qos, boolean retain, boolean dup,
                            int packetId, Mqtt5PublishProperties properties) {
        this.topic = topic;
        this.payload = MqttMessageBuilder.cloneBytes(payload);
        this.qos = qos;
        this.retain = retain;
        this.dup = dup;
        this.packetId = packetId;
        this.properties = properties != null ? properties : Mqtt5PublishProperties.builder().build();
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
    public Mqtt5PublishProperties getProperties() {
        return properties;
    }

    @Override
    public Mqtt5PublishBuilder toBuilder() {
        return Mqtt5Publish.builder()
                .topic(topic)
                .payload(payload)
                .qos(qos)
                .retain(retain)
                .dup(dup)
                .packetId(packetId)
                .properties(properties);
    }
}
