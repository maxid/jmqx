package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.QoS;

/**
 * {@link Mqtt3Publish} 的可变 builder。
 *
 * <p>使用 builder 模式构建不可变的 {@link Mqtt3Publish} 实例。
 *
 * @author maxid
 * @since 1.4.14
 */
public class Mqtt3PublishBuilder {

    /** 主题名 */
    private String  topic;
    /** 消息负载 */
    private byte[]  payload;
    /** QoS 等级，默认为 AT_MOST_ONCE */
    private QoS     qos      = QoS.AT_MOST_ONCE;
    /** 保留标志，默认为 false */
    private boolean retain   = false;
    /** 重复标志，默认为 false */
    private boolean dup      = false;
    /** 报文标识符，默认为 0 */
    private int     packetId = 0;

    /**
     * 设置主题名。
     *
     * @param topic 主题名
     * @return this builder
     */
    public Mqtt3PublishBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * 设置消息负载。
     *
     * @param payload 消息负载字节数组
     * @return this builder
     */
    public Mqtt3PublishBuilder payload(byte[] payload) {
        this.payload = payload;
        return this;
    }

    /**
     * 设置 QoS 等级。
     *
     * @param qos QoS 等级
     * @return this builder
     */
    public Mqtt3PublishBuilder qos(QoS qos) {
        this.qos = qos;
        return this;
    }

    /**
     * 设置保留标志。
     *
     * @param retain 保留标志
     * @return this builder
     */
    public Mqtt3PublishBuilder retain(boolean retain) {
        this.retain = retain;
        return this;
    }

    /**
     * 设置重复标志。
     *
     * @param dup DUP 标志
     * @return this builder
     */
    public Mqtt3PublishBuilder dup(boolean dup) {
        this.dup = dup;
        return this;
    }

    /**
     * 设置报文标识符。
     *
     * @param packetId 报文标识符
     * @return this builder
     */
    public Mqtt3PublishBuilder packetId(int packetId) {
        this.packetId = packetId;
        return this;
    }

    /**
     * 构建不可变的 {@link Mqtt3Publish} 实例。
     *
     * @return 不可变的 {@link Mqtt3Publish} 实例
     */
    public Mqtt3Publish build() {
        return new Mqtt3PublishImpl(topic, payload, qos, retain, dup, packetId);
    }

}
