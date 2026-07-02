package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v3.internal.DefaultMqtt3Client;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * MQTT 3 客户端 builder。
 *
 * <p>镜像 hivemq {@code Mqtt3ClientBuilder} 的 API 表面。
 *
 * @author maxid
 */
public class Mqtt3ClientBuilder {

    private final Mqtt3ClientConfig config = new Mqtt3ClientConfig();
    private final List<MqttClientConnectedListener> connected = new ArrayList<>();
    private final List<MqttClientDisconnectedListener> disconnected = new ArrayList<>();

    public Mqtt3ClientBuilder serverHost(String host) {
        config.setServerHost(host);
        return this;
    }

    public Mqtt3ClientBuilder serverPort(int port) {
        config.setServerPort(port);
        return this;
    }

    public Mqtt3ClientBuilder identifier(String clientId) {
        config.setClientId(clientId);
        return this;
    }

    public Mqtt3ClientBuilder identifier() {
        config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        return this;
    }

    public Mqtt3ClientBuilder keepAliveSeconds(int seconds) {
        config.setKeepAliveSeconds(seconds);
        return this;
    }

    public Mqtt3ClientBuilder cleanSession(boolean cleanSession) {
        config.setCleanSession(cleanSession);
        return this;
    }

    public Mqtt3ClientBuilder username(String username) {
        config.setUsername(username);
        return this;
    }

    public Mqtt3ClientBuilder password(byte[] password) {
        config.setPassword(password);
        return this;
    }

    public Mqtt3ClientBuilder willPublish(Mqtt3Publish willPublish) {
        config.setWillPublish(willPublish);
        return this;
    }

    public Mqtt3ClientBuilder automaticReconnect() {
        config.setAutomaticReconnect(true);
        return this;
    }

    public Mqtt3ClientBuilder addConnectedListener(MqttClientConnectedListener listener) {
        connected.add(listener);
        return this;
    }

    public Mqtt3ClientBuilder addDisconnectedListener(MqttClientDisconnectedListener listener) {
        disconnected.add(listener);
        return this;
    }

    public Mqtt3ClientBuilder transportType(MqttClientConfig.TransportType type) {
        config.setTransportType(type);
        return this;
    }

    private void ensureClientId() {
        if (config.getClientId() == null || config.getClientId().isEmpty()) {
            config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        }
        config.setVersion(MqttVersion.MQTT_3_1_1);
    }

    public Mqtt3RxClient buildRx() {
        ensureClientId();
        return new DefaultMqtt3Client(config, connected, disconnected);
    }

    public Mqtt3AsyncClient buildAsync() {
        ensureClientId();
        return buildRx().toAsync();
    }

    public Mqtt3BlockingClient buildBlocking() {
        ensureClientId();
        return buildRx().toBlock();
    }
}
