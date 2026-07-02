package plus.jmqx.client.mqtt;

/**
 * MQTT 客户端入口 —— 通过 {@link #builder()} 选择协议版本。
 *
 * @author maxid
 * @since 1.4.14
 */
public interface MqttClient {

    /**
     * 默认服务器主机。
     */
    String DEFAULT_SERVER_HOST               = "localhost";
    /**
     * 默认 MQTT 端口。
     */
    int    DEFAULT_SERVER_PORT               = 1883;
    /**
     * SSL/TLS 默认端口。
     */
    int    DEFAULT_SERVER_PORT_SSL           = 8883;
    /**
     * WebSocket 默认端口。
     */
    int    DEFAULT_SERVER_PORT_WEBSOCKET     = 80;
    /**
     * WebSocket + SSL 默认端口。
     */
    int    DEFAULT_SERVER_PORT_WEBSOCKET_SSL = 443;

    /**
     * 创建顶层客户端 builder，用于选择 MQTT 协议版本。
     *
     * @return 版本选择 builder
     */
    static MqttClientBuilder builder() {
        return new MqttClientBuilder();
    }

    /**
     * @return 当前客户端的运行时配置（只读视图）。
     */
    MqttClientConfig getConfig();

    /**
     * @return 当前连接状态。
     */
    MqttClientState getState();

    /**
     * @return 协商的 MQTT 协议版本。
     */
    MqttVersion getVersion();

}
