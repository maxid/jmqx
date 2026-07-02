package plus.jmqx.client.mqtt.v5.message;

import plus.jmqx.client.mqtt.message.QoS;

/**
 * {@link Mqtt5Publish} 的可变 builder。
 *
 * @author maxid
 * @since 1.4.14
 */
public class Mqtt5PublishBuilder {

    private String                 topic;
    private byte[]                 payload;
    private QoS                    qos        = QoS.AT_MOST_ONCE;
    private boolean                retain;
    private boolean                dup;
    private int                    packetId;
    private Mqtt5PublishProperties properties = Mqtt5PublishProperties.builder().build();

    /**
     * @param topic 主题名。 @return this builder
     */
    public Mqtt5PublishBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * @param payload 消息负载。 @return this builder
     */
    public Mqtt5PublishBuilder payload(byte[] payload) {
        this.payload = payload;
        return this;
    }

    /**
     * @param qos QoS 等级。 @return this builder
     */
    public Mqtt5PublishBuilder qos(QoS qos) {
        this.qos = qos;
        return this;
    }

    /**
     * @param retain 保留标志。 @return this builder
     */
    public Mqtt5PublishBuilder retain(boolean retain) {
        this.retain = retain;
        return this;
    }

    /**
     * @param dup DUP 标志。 @return this builder
     */
    public Mqtt5PublishBuilder dup(boolean dup) {
        this.dup = dup;
        return this;
    }

    /**
     * @param packetId 报文标识符。 @return this builder
     */
    public Mqtt5PublishBuilder packetId(int packetId) {
        this.packetId = packetId;
        return this;
    }

    /**
     * @param properties MQTT 5 PUBLISH 属性。 @return this builder
     */
    public Mqtt5PublishBuilder properties(Mqtt5PublishProperties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * @return 不可变的 {@link Mqtt5Publish} 实例
     */
    public Mqtt5Publish build() {
        return new Mqtt5PublishImpl(topic, payload, qos, retain, dup, packetId, properties);
    }

}
