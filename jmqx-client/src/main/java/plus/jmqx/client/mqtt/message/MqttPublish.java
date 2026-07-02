package plus.jmqx.client.mqtt.message;

/**
 * 版本无关的 PUBLISH 消息。
 *
 * <p>出站与入站共用此接口；入站消息由引擎的 {@code MqttInbox} 包装为 {@code Deliverable}，
 * 其 {@link #ack()} 实现会在下游消费时触发真正的 PUBACK/PUBREC 回调。
 *
 * @author maxid
 */
public interface MqttPublish {

    String getTopic();

    byte[] getPayloadAsBytes();

    QoS getQoS();

    boolean isRetain();

    boolean isDup();

    int getPacketId();

    /**
     * 确认该入站消息。对于 QoS1/2，引擎的 inbox 会在订阅者消费消息后向 broker 发送
     * PUBACK/PUBREC；对于 QoS0 为 no-op。对出站 PUBLISH 调用为 no-op。可安全多次调用。
     *
     * <p>默认实现为 no-op；引擎的 {@code Deliverable} 包装器覆盖此方法以触发真正的 ACK 回调。
     */
    default void ack() {
    }
}
