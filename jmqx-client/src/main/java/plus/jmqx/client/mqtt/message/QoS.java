package plus.jmqx.client.mqtt.message;

/**
 * MQTT 服务质量等级。
 *
 * @author maxid
 */
public enum QoS {
    /** 至多一次（fire and forget）。 */
    AT_MOST_ONCE(0),
    /** 至少一次（PUBACK 确认）。 */
    AT_LEAST_ONCE(1),
    /** 恰好一次（PUBREC/PUBREL/PUBCOMP 两阶段握手）。 */
    EXACTLY_ONCE(2);

    private final int value;

    QoS(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static QoS fromValue(int value) {
        return switch (value) {
            case 0 -> AT_MOST_ONCE;
            case 1 -> AT_LEAST_ONCE;
            case 2 -> EXACTLY_ONCE;
            default -> throw new IllegalArgumentException("Invalid QoS value: " + value);
        };
    }
}
