package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.QoS;

/**
 * {@link Mqtt3Publish} 的可变 builder。
 *
 * @author maxid
 * @since 1.4.14
 */
public class Mqtt3PublishBuilder {

    private String  topic;
    private byte[]  payload;
    private QoS     qos      = QoS.AT_MOST_ONCE;
    private boolean retain   = false;
    private boolean dup      = false;
    private int     packetId = 0;

    /**
     * @param topic 主题名。 @return this builder
     */
    public Mqtt3PublishBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * @param payload 消息负载。 @return this builder
     */
    public Mqtt3PublishBuilder payload(byte[] payload) {
        this.payload = payload;
        return this;
    }

    /**
     * @param qos QoS 等级。 @return this builder
     */
    public Mqtt3PublishBuilder qos(QoS qos) {
        this.qos = qos;
        return this;
    }

    /**
     * @param retain 保留标志。 @return this builder
     */
    public Mqtt3PublishBuilder retain(boolean retain) {
        this.retain = retain;
        return this;
    }

    /**
     * @param dup DUP 标志。 @return this builder
     */
    public Mqtt3PublishBuilder dup(boolean dup) {
        this.dup = dup;
        return this;
    }

    /**
     * @param packetId 报文标识符。 @return this builder
     */
    public Mqtt3PublishBuilder packetId(int packetId) {
        this.packetId = packetId;
        return this;
    }

    /**
     * @return 不可变的 {@link Mqtt3Publish} 实例
     */
    public Mqtt3Publish build() {
        return new Mqtt3PublishImpl(topic, payload, qos, retain, dup, packetId);
    }

}
