package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.MqttMessageBuilder;
import plus.jmqx.client.mqtt.message.QoS;

/**
 * 不可变的 {@link Mqtt3Publish} 实现。
 *
 * <p>由 {@link Mqtt3PublishBuilder} 构建，所有字段在构造后不可修改。
 *
 * @author maxid
 */
public final class Mqtt3PublishImpl implements Mqtt3Publish {

    /**
     * 主题名
     */
    private final String  topic;
    /**
     * 消息负载字节数组
     */
    private final byte[]  payload;
    /**
     * QoS 等级
     */
    private final QoS     qos;
    /**
     * 保留标志
     */
    private final boolean retain;
    /**
     * 重复标志
     */
    private final boolean dup;
    /**
     * 报文标识符
     */
    private final int     packetId;

    /**
     * 构造不可变的 PUBLISH 实例。
     *
     * @param topic    主题名
     * @param payload  消息负载字节数组（内部会克隆）
     * @param qos      QoS 等级
     * @param retain   保留标志
     * @param dup      重复标志
     * @param packetId 报文标识符
     */
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
