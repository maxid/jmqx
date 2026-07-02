package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.v5.internal.DefaultMqtt5Client;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * MQTT 5 客户端 builder。
 *
 * @author maxid
 */
public class Mqtt5ClientBuilder {

    private final Mqtt5ClientConfig config = new Mqtt5ClientConfig();
    private final List<MqttClientConnectedListener> connected = new ArrayList<>();
    private final List<MqttClientDisconnectedListener> disconnected = new ArrayList<>();

    public Mqtt5ClientBuilder serverHost(String host) {
        config.setServerHost(host);
        return this;
    }

    public Mqtt5ClientBuilder serverPort(int port) {
        config.setServerPort(port);
        return this;
    }

    public Mqtt5ClientBuilder identifier(String clientId) {
        config.setClientId(clientId);
        return this;
    }

    public Mqtt5ClientBuilder identifier() {
        config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        return this;
    }

    public Mqtt5ClientBuilder keepAliveSeconds(int seconds) {
        config.setKeepAliveSeconds(seconds);
        return this;
    }

    public Mqtt5ClientBuilder cleanStart(boolean cleanStart) {
        config.setCleanSession(cleanStart);
        return this;
    }

    public Mqtt5ClientBuilder sessionExpiryInterval(long seconds) {
        config.setSessionExpiryInterval(seconds);
        return this;
    }

    public Mqtt5ClientBuilder receiveMaximum(int receiveMaximum) {
        config.setReceiveMaximum(receiveMaximum);
        return this;
    }

    public Mqtt5ClientBuilder username(String username) {
        config.setUsername(username);
        return this;
    }

    public Mqtt5ClientBuilder password(byte[] password) {
        config.setPassword(password);
        return this;
    }

    public Mqtt5ClientBuilder willPublish(Mqtt5Publish willPublish) {
        config.setWillPublish(willPublish);
        return this;
    }

    public Mqtt5ClientBuilder automaticReconnect() {
        config.setAutomaticReconnect(true);
        return this;
    }

    public Mqtt5ClientBuilder addConnectedListener(MqttClientConnectedListener listener) {
        connected.add(listener);
        return this;
    }

    public Mqtt5ClientBuilder addDisconnectedListener(MqttClientDisconnectedListener listener) {
        disconnected.add(listener);
        return this;
    }

    public Mqtt5ClientBuilder transportType(MqttClientConfig.TransportType type) {
        config.setTransportType(type);
        return this;
    }

    private void ensureClientId() {
        if (config.getClientId() == null || config.getClientId().isEmpty()) {
            config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        }
        config.setVersion(MqttVersion.MQTT_5);
    }

    public Mqtt5RxClient buildRx() {
        ensureClientId();
        return new DefaultMqtt5Client(config, connected, disconnected);
    }

    public Mqtt5AsyncClient buildAsync() {
        ensureClientId();
        return buildRx().toAsync();
    }

    public Mqtt5BlockingClient buildBlocking() {
        ensureClientId();
        return buildRx().toBlock();
    }
}
