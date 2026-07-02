package plus.jmqx.client.mqtt.lifecycle;

import lombok.Getter;
import plus.jmqx.client.mqtt.MqttClientConfig;

/**
 * {@link MqttClientDisconnectedListener} 的上下文。
 *
 * @author maxid
 * @since 1.4.14
 */
@Getter
public final class MqttClientDisconnectedContext {

    /**
     * 断开来源。
     */
    public enum DisconnectSource {
        /**
         * 用户主动断开（调用 disconnect）。
         */
        USER,
        /**
         * 客户端侧异常。
         */
        CLIENT,
        /**
         * 服务端侧断开（DISCONNECT 或连接丢失）。
         */
        SERVER
    }

    /**
     * 客户端配置
     */
    private final MqttClientConfig      clientConfig;
    /**
     * 断开来源
     */
    private final DisconnectSource      source;
    /**
     * 断开原因；可能为 {@code null}
     */
    private final Throwable             cause;
    /**
     * 重连控制器，可在监听器中修改
     */
    private final MqttClientReconnector reconnector;

    /**
     * @param clientConfig 客户端配置
     * @param source       断开来源
     * @param cause        断开原因；可能为 {@code null}
     * @param reconnector  重连控制器
     */
    public MqttClientDisconnectedContext(MqttClientConfig clientConfig, DisconnectSource source,
                                         Throwable cause, MqttClientReconnector reconnector) {
        this.clientConfig = clientConfig;
        this.source = source;
        this.cause = cause;
        this.reconnector = reconnector;
    }

}
