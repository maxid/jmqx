package plus.jmqx.client.mqtt.v3.message;

/**
 * MQTT 3.1.1 CONNACK 返回码（协议规范 §3.2.2.3）。
 *
 * <p>包含协议约定的 6 种连接返回码值。
 *
 * @author maxid
 */
public enum Mqtt3ConnAckReturnCode {

    /**
     * 连接已接受
     */
    ACCEPTED(0),
    /**
     * 不接受的协议版本
     */
    UNACCEPTABLE_PROTOCOL_VERSION(1),
    /**
     * 标识符被拒绝
     */
    IDENTIFIER_REJECTED(2),
    /**
     * 服务端不可用
     */
    SERVER_UNAVAILABLE(3),
    /**
     * 用户名或密码错误
     */
    BAD_USERNAME_OR_PASSWORD(4),
    /**
     * 未授权
     */
    NOT_AUTHORIZED(5);

    /**
     * 返回码数值
     */
    private final int code;

    Mqtt3ConnAckReturnCode(int code) {
        this.code = code;
    }

    /**
     * 获取返回码数值。
     *
     * @return 返回码数值
     */
    public int code() {
        return code;
    }

    /**
     * 根据数值解析返回码。
     *
     * @param code 返回码数值
     * @return 对应的枚举值
     * @throws IllegalArgumentException 未知的返回码
     */
    public static Mqtt3ConnAckReturnCode fromCode(int code) {
        for (var v : values()) {
            if (v.code == code) {
                return v;
            }
        }
        throw new IllegalArgumentException("未知 CONNACK 返回码: " + code);
    }

    /**
     * 判断是否连接已接受。
     *
     * @return 如果返回码为 ACCEPTED 则返回 true
     */
    public boolean isAccepted() {
        return this == ACCEPTED;
    }

}
