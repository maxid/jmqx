package plus.jmqx.client.mqtt;

/**
 * 客户端连接状态机（4 态）。
 *
 * @author maxid
 */
public enum MqttClientState {
    /** 已断开，未连接也未尝试连接。 */
    DISCONNECTED,
    /** 正在建立传输连接与 MQTT CONNECT 握手。 */
    CONNECTING,
    /** MQTT CONNACK 已接收，会话已建立。 */
    CONNECTED,
    /** 正在发送 DISCONNECT 并关闭传输。 */
    DISCONNECTING
}
