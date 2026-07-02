package plus.jmqx.client.mqtt.message;

/**
 * MQTT 服务质量等级。
 *
 * @author maxid
 * @since 1.4.14
 */
public enum QoS {
    /**
     * 至多一次（fire and forget）。
     */
    AT_MOST_ONCE(0),
    /**
     * 至少一次（PUBACK 确认）。
     */
    AT_LEAST_ONCE(1),
    /**
     * 恰好一次（PUBREC/PUBREL/PUBCOMP 两阶段握手）。
     */
    EXACTLY_ONCE(2);

    private final int value;

    QoS(int value) {
        this.value = value;
    }

    /**
     * 获取 MQTT 协议中的 QoS 数值（0/1/2）
     *
     * @return MQTT 协议中的 QoS 数值（0/1/2）
     */
    public int value() {
        return value;
    }

    /**
     * 根据传入的 MQTT 协议 QoS 数值获得对应的枚举常量
     *
     * @param value MQTT 协议 QoS 数值
     * @return 对应的枚举常量
     * @throws IllegalArgumentException 若 value 不在 0..2 范围内
     */
    public static QoS fromValue(int value) {
        return switch (value) {
            case 0 -> AT_MOST_ONCE;
            case 1 -> AT_LEAST_ONCE;
            case 2 -> EXACTLY_ONCE;
            default -> throw new IllegalArgumentException("Invalid QoS value: " + value);
        };
    }

}
