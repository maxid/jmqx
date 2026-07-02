package plus.jmqx.client.mqtt.lifecycle;

import plus.jmqx.client.mqtt.MqttClientConfig;

/**
 * {@link MqttClientConnectedListener} 的上下文。
 *
 * @author maxid
 */
public final class MqttClientConnectedContext {

    private final MqttClientConfig clientConfig;
    private final boolean sessionPresent;

    public MqttClientConnectedContext(MqttClientConfig clientConfig, boolean sessionPresent) {
        this.clientConfig = clientConfig;
        this.sessionPresent = sessionPresent;
    }

    public MqttClientConfig getClientConfig() {
        return clientConfig;
    }

    public boolean isSessionPresent() {
        return sessionPresent;
    }
}
