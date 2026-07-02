package plus.jmqx.client.mqtt.v3.message;

/**
 * MQTT 3.1.1 CONNACK 返回码（spec §3.2.2.3）。
 *
 * @author maxid
 */
public enum Mqtt3ConnAckReturnCode {

    ACCEPTED(0),
    UNACCEPTABLE_PROTOCOL_VERSION(1),
    IDENTIFIER_REJECTED(2),
    SERVER_UNAVAILABLE(3),
    BAD_USERNAME_OR_PASSWORD(4),
    NOT_AUTHORIZED(5);

    private final int code;

    Mqtt3ConnAckReturnCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static Mqtt3ConnAckReturnCode fromCode(int code) {
        for (var v : values()) {
            if (v.code == code) {
                return v;
            }
        }
        throw new IllegalArgumentException("Unknown CONNACK return code: " + code);
    }

    public boolean isAccepted() {
        return this == ACCEPTED;
    }
}
