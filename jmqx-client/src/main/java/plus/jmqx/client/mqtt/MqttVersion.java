package plus.jmqx.client.mqtt;

/**
 * MQTT 协议版本。
 *
 * @author maxid
 */
public enum MqttVersion {
    /** MQTT 3.1。 */
    MQTT_3_1(3, "MQIsdp", (byte) 3),
    /** MQTT 3.1.1。 */
    MQTT_3_1_1(4, "MQTT", (byte) 4),
    /** MQTT 5.0。 */
    MQTT_5(5, "MQTT", (byte) 5);

    private final int level;
    private final String name;
    private final byte protocolLevel;

    MqttVersion(int level, String name, byte protocolLevel) {
        this.level = level;
        this.name = name;
        this.protocolLevel = protocolLevel;
    }

    public int level() {
        return level;
    }

    public String protocolName() {
        return name;
    }

    public byte protocolLevel() {
        return protocolLevel;
    }
}
