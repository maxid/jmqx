package plus.jmqx.client.mqtt.v3.message;

/**
 * MQTT 3.1.1 DISCONNECT —— 无字段（v3 无 reason code）。
 *
 * @author maxid
 */
public final class Mqtt3Disconnect {

    private Mqtt3Disconnect() {
    }

    public static final Mqtt3Disconnect INSTANCE = new Mqtt3Disconnect();
}
