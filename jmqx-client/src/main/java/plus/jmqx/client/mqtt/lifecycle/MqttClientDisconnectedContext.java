package plus.jmqx.client.mqtt.lifecycle;

import plus.jmqx.client.mqtt.MqttClientConfig;

/**
 * {@link MqttClientDisconnectedListener} 的上下文。
 *
 * @author maxid
 */
public final class MqttClientDisconnectedContext {

    /** 断开来源。 */
    public enum DisconnectSource {
        /** 用户主动断开（调用 disconnect）。 */
        USER,
        /** 客户端侧异常。 */
        CLIENT,
        /** 服务端侧断开（DISCONNECT 或连接丢失）。 */
        SERVER
    }

    private final MqttClientConfig clientConfig;
    private final DisconnectSource source;
    private final Throwable cause;
    private final MqttClientReconnector reconnector;

    public MqttClientDisconnectedContext(MqttClientConfig clientConfig, DisconnectSource source,
                                         Throwable cause, MqttClientReconnector reconnector) {
        this.clientConfig = clientConfig;
        this.source = source;
        this.cause = cause;
        this.reconnector = reconnector;
    }

    public MqttClientConfig getClientConfig() {
        return clientConfig;
    }

    public DisconnectSource getSource() {
        return source;
    }

    public Throwable getCause() {
        return cause;
    }

    public MqttClientReconnector getReconnector() {
        return reconnector;
    }
}
