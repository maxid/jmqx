package plus.jmqx.client.mqtt.lifecycle;

import lombok.Getter;
import plus.jmqx.client.mqtt.MqttClientConfig;

/**
 * {@link MqttClientConnectedListener} 的上下文。
 *
 * @author maxid
 * @since 1.4.14
 */
@Getter
public final class MqttClientConnectedContext {

    /**
     * 客户端配置
     */
    private final MqttClientConfig clientConfig;
    /**
     * 是否保留了上一次会话
     */
    private final boolean          sessionPresent;

    /**
     * @param clientConfig   客户端配置
     * @param sessionPresent CONNACK 中的 sessionPresent 标志
     */
    public MqttClientConnectedContext(MqttClientConfig clientConfig, boolean sessionPresent) {
        this.clientConfig = clientConfig;
        this.sessionPresent = sessionPresent;
    }

}
