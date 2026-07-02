package plus.jmqx.client.mqtt;

/**
 * MQTT 客户端入口 —— 通过 builder() 选择版本。
 *
 * @author maxid
 */
public interface MqttClient {

    /** 默认服务器主机。 */
    String DEFAULT_SERVER_HOST = "localhost";
    /** 默认服务器端口。 */
    int DEFAULT_SERVER_PORT = 1883;
    /** SSL/TLS 默认端口。 */
    int DEFAULT_SERVER_PORT_SSL = 8883;
    /** WebSocket 默认端口。 */
    int DEFAULT_SERVER_PORT_WEBSOCKET = 80;
    /** WebSocket + SSL 默认端口。 */
    int DEFAULT_SERVER_PORT_WEBSOCKET_SSL = 443;

    /**
     * 创建客户端 builder。
     */
    static MqttClientBuilder builder() {
        return new MqttClientBuilder();
    }

    MqttClientConfig getConfig();

    /**
     * @return 客户端状态。
     */
    MqttClientState getState();

    /**
     * @return 协议版本。
     */
    MqttVersion getVersion();
}
