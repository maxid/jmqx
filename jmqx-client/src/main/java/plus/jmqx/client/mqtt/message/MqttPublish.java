package plus.jmqx.client.mqtt.message;

/**
 * 版本无关的 PUBLISH 消息。
 *
 * <p>出站与入站共用此接口；入站消息由引擎的 inbox 包装为 {@code Deliverable}，
 * 其 {@link #ack()} 实现会在下游消费时触发真正的 PUBACK/PUBREC 回调。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface MqttPublish {

    /**
     * 获取主题名
     *
     * @return 主题名
     */
    String getTopic();

    /**
     * 获取消息负载字节数组
     *
     * @return 消息负载字节数组
     */
    byte[] getPayloadAsBytes();

    /**
     * 获取QoS 等级
     *
     * @return QoS 等级
     */
    QoS getQoS();

    /**
     * 获取是否为保留消息
     *
     * @return 是否为保留消息
     */
    boolean isRetain();

    /**
     * 获取 DUP 标志
     *
     * @return DUP 标志
     */
    boolean isDup();

    /**
     * 获取 MQTT 报文标识符
     *
     * @return MQTT 报文标识符；QoS0 时为 0
     */
    int getPacketId();

    /**
     * 确认该入站消息。对于 QoS1/2，引擎的 inbox 会在订阅者消费消息后向 broker 发送
     * PUBACK/PUBREC；对于 QoS0 为 no-op。对出站 PUBLISH 调用为 no-op。可安全多次调用。
     */
    default void ack() {
    }

}
