package plus.jmqx.client.mqtt.v3;

import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.MqttVersion;
import plus.jmqx.client.mqtt.lifecycle.MqttClientConnectedListener;
import plus.jmqx.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import plus.jmqx.client.mqtt.v3.internal.DefaultMqtt3Client;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * MQTT 3.1.1 客户端 builder。
 *
 * <p>API 风格对标 HiveMQ MQTT Client {@code Mqtt3ClientBuilder}。
 *
 * @author maxid
 * @since 1.4.14
 */
public class Mqtt3ClientBuilder {

    private final Mqtt3ClientConfig                    config       = new Mqtt3ClientConfig();
    private final List<MqttClientConnectedListener>    connected    = new ArrayList<>();
    private final List<MqttClientDisconnectedListener> disconnected = new ArrayList<>();

    /**
     * @param host MQTT broker 主机名或 IP。 @return this builder
     */
    public Mqtt3ClientBuilder serverHost(String host) {
        config.setServerHost(host);
        return this;
    }

    /**
     * @param port MQTT broker 端口。 @return this builder
     */
    public Mqtt3ClientBuilder serverPort(int port) {
        config.setServerPort(port);
        return this;
    }

    /**
     * @param clientId MQTT 客户端标识符。 @return this builder
     */
    public Mqtt3ClientBuilder identifier(String clientId) {
        config.setClientId(clientId);
        return this;
    }

    /**
     * 生成随机客户端标识符（{@code jmqx-<uuid8>}）。 @return this builder
     */
    public Mqtt3ClientBuilder identifier() {
        config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        return this;
    }

    /**
     * @param seconds Keep Alive 间隔（秒）。 @return this builder
     */
    public Mqtt3ClientBuilder keepAliveSeconds(int seconds) {
        config.setKeepAliveSeconds(seconds);
        return this;
    }

    /**
     * @param cleanSession MQTT cleanSession 标志。 @return this builder
     */
    public Mqtt3ClientBuilder cleanSession(boolean cleanSession) {
        config.setCleanSession(cleanSession);
        return this;
    }

    /**
     * @param username 认证用户名。 @return this builder
     */
    public Mqtt3ClientBuilder username(String username) {
        config.setUsername(username);
        return this;
    }

    /**
     * @param password 认证密码。 @return this builder
     */
    public Mqtt3ClientBuilder password(byte[] password) {
        config.setPassword(password);
        return this;
    }

    /**
     * @param willPublish 遗嘱消息。 @return this builder
     */
    public Mqtt3ClientBuilder willPublish(Mqtt3Publish willPublish) {
        config.setWillPublish(willPublish);
        return this;
    }

    /**
     * 启用自动重连（指数退避）。 @return this builder
     */
    public Mqtt3ClientBuilder automaticReconnect() {
        config.setAutomaticReconnect(true);
        return this;
    }

    /**
     * @param listener 连接成功监听器。 @return this builder
     */
    public Mqtt3ClientBuilder addConnectedListener(MqttClientConnectedListener listener) {
        connected.add(listener);
        return this;
    }

    /**
     * @param listener 断开连接监听器。 @return this builder
     */
    public Mqtt3ClientBuilder addDisconnectedListener(MqttClientDisconnectedListener listener) {
        disconnected.add(listener);
        return this;
    }

    /**
     * @param type 传输层类型（TCP/TLS/WS/WSS）。 @return this builder
     */
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

    /**
     * @return Reactor API 客户端（Mono/Flux）
     */
    public Mqtt3RxClient buildRx() {
        ensureClientId();
        return new DefaultMqtt3Client(config, connected, disconnected);
    }

    /**
     * @return CompletableFuture 异步 API 客户端
     */
    public Mqtt3AsyncClient buildAsync() {
        ensureClientId();
        return buildRx().toAsync();
    }

    /**
     * @return 阻塞 API 客户端
     */
    public Mqtt3BlockingClient buildBlocking() {
        ensureClientId();
        return buildRx().toBlock();
    }

}
