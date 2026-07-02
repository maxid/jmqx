package plus.jmqx.client.mqtt;

/**
 * MQTT 协议版本。
 *
 * @author maxid
 * @since 1.4.14
 */
public enum MqttVersion {
    /**
     * MQTT 3.1。
     */
    MQTT_3_1(3, "MQIsdp", (byte) 3),
    /**
     * MQTT 3.1.1。
     */
    MQTT_3_1_1(4, "MQTT", (byte) 4),
    /**
     * MQTT 5.0。
     */
    MQTT_5(5, "MQTT", (byte) 5);

    /**
     * 内部版本序号
     */
    private final int    level;
    /**
     * 协议名称（MQIsdp / MQTT）
     */
    private final String name;
    /**
     * 协议级别字节
     */
    private final byte   protocolLevel;

    /**
     * 构造 MQTT 协议版本枚举常量
     *
     * @param level         内部版本序号
     * @param name          CONNECT 报文中的协议名称
     * @param protocolLevel CONNECT 报文中的协议级别字节
     */
    MqttVersion(int level, String name, byte protocolLevel) {
        this.level = level;
        this.name = name;
        this.protocolLevel = protocolLevel;
    }

    /**
     * @return 内部版本序号。
     */
    public int level() {
        return level;
    }

    /**
     * @return CONNECT 报文中的协议名称字符串。
     */
    public String protocolName() {
        return name;
    }

    /**
     * @return CONNECT 报文中的协议级别字节。
     */
    public byte protocolLevel() {
        return protocolLevel;
    }

}
