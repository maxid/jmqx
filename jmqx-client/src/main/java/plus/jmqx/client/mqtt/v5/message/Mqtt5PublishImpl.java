package plus.jmqx.client.mqtt.v5.message;

import plus.jmqx.client.mqtt.message.MqttMessageBuilder;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * {@link Mqtt5Publish} 的不可变实现。
 *
 * @author maxid
 */
public final class Mqtt5PublishImpl implements Mqtt5Publish {

    /** 主题名 */
    private final String                 topic;
    /** 消息负载 */
    private final byte[]                 payload;
    /** QoS 等级 */
    private final QoS                    qos;
    /** 保留标志 */
    private final boolean                retain;
    /** DUP 标志 */
    private final boolean                dup;
    /** 报文标识符 */
    private final int                    packetId;
    /** MQTT 5 PUBLISH 属性 */
    private final Mqtt5PublishProperties properties;

    /**
     * 构造不可变 PUBLISH 实例。
     *
     * @param topic      主题名
     * @param payload    消息负载
     * @param qos        QoS 等级
     * @param retain     保留标志
     * @param dup        DUP 标志
     * @param packetId   报文标识符
     * @param properties MQTT 5 PUBLISH 属性（为空则使用默认值）
     */
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
