package plus.jmqx.client.mqtt.v3.message;

/**
 * MQTT 3.1.1 DISCONNECT 报文。
 *
 * <p>MQTT 3.1.1 版本的 DISCONNECT 不包含 reason code，仅为空报文。
 *
 * @author maxid
 */
public final class Mqtt3Disconnect {

    /**
     * 单例实例
     */
    public static final Mqtt3Disconnect INSTANCE = new Mqtt3Disconnect();

    private Mqtt3Disconnect() {
    }

}
