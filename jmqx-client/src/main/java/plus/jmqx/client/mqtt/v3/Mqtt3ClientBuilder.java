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

    /**
     * MQTT 3 客户端配置
     */
    private final Mqtt3ClientConfig                    config       = new Mqtt3ClientConfig();
    /**
     * 连接成功监听器列表
     */
    private final List<MqttClientConnectedListener>    connected    = new ArrayList<>();
    /**
     * 断开连接监听器列表
     */
    private final List<MqttClientDisconnectedListener> disconnected = new ArrayList<>();

    /**
     * 设置 MQTT broker 主机名或 IP。
     *
     * @param host MQTT broker 主机名或 IP
     * @return this builder
     */
    public Mqtt3ClientBuilder serverHost(String host) {
        config.setServerHost(host);
        return this;
    }

    /**
     * 设置 MQTT broker 端口。
     *
     * @param port MQTT broker 端口
     * @return this builder
     */
    public Mqtt3ClientBuilder serverPort(int port) {
        config.setServerPort(port);
        return this;
    }

    /**
     * 设置客户端标识符。
     *
     * @param clientId MQTT 客户端标识符
     * @return this builder
     */
    public Mqtt3ClientBuilder identifier(String clientId) {
        config.setClientId(clientId);
        return this;
    }

    /**
     * 生成并设置随机客户端标识符（格式：{@code jmqx-<uuid8>}）。
     *
     * @return this builder
     */
    public Mqtt3ClientBuilder identifier() {
        config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        return this;
    }

    /**
     * 设置 Keep Alive 间隔。
     *
     * @param seconds Keep Alive 间隔（秒）
     * @return this builder
     */
    public Mqtt3ClientBuilder keepAliveSeconds(int seconds) {
        config.setKeepAliveSeconds(seconds);
        return this;
    }

    /**
     * 设置 cleanSession 标志。
     *
     * @param cleanSession MQTT cleanSession 标志
     * @return this builder
     */
    public Mqtt3ClientBuilder cleanSession(boolean cleanSession) {
        config.setCleanSession(cleanSession);
        return this;
    }

    /**
     * 设置认证用户名。
     *
     * @param username 认证用户名
     * @return this builder
     */
    public Mqtt3ClientBuilder username(String username) {
        config.setUsername(username);
        return this;
    }

    /**
     * 设置认证密码。
     *
     * @param password 认证密码
     * @return this builder
     */
    public Mqtt3ClientBuilder password(byte[] password) {
        config.setPassword(password);
        return this;
    }

    /**
     * 设置遗嘱消息。
     *
     * @param willPublish 遗嘱消息
     * @return this builder
     */
    public Mqtt3ClientBuilder willPublish(Mqtt3Publish willPublish) {
        config.setWillPublish(willPublish);
        return this;
    }

    /**
     * 启用自动重连（使用指数退避策略）。
     *
     * @return this builder
     */
    public Mqtt3ClientBuilder automaticReconnect() {
        config.setAutomaticReconnect(true);
        return this;
    }

    /**
     * 添加连接成功监听器。
     *
     * @param listener 连接成功监听器
     * @return this builder
     */
    public Mqtt3ClientBuilder addConnectedListener(MqttClientConnectedListener listener) {
        connected.add(listener);
        return this;
    }

    /**
     * 添加断开连接监听器。
     *
     * @param listener 断开连接监听器
     * @return this builder
     */
    public Mqtt3ClientBuilder addDisconnectedListener(MqttClientDisconnectedListener listener) {
        disconnected.add(listener);
        return this;
    }

    /**
     * 设置传输层类型。
     *
     * @param type 传输层类型（TCP/TLS/WS/WSS）
     * @return this builder
     */
    public Mqtt3ClientBuilder transportType(MqttClientConfig.TransportType type) {
        config.setTransportType(type);
        return this;
    }

    /**
     * 设置 TLS 配置。
     *
     * @param sslConfig TLS 配置
     * @return this builder
     */
    public Mqtt3ClientBuilder sslConfig(plus.jmqx.client.mqtt.internal.transport.MqttSslConfig sslConfig) {
        config.setSslConfig(sslConfig);
        return this;
    }

    /**
     * 设置 WebSocket 配置。
     *
     * @param webSocketConfig WebSocket 配置
     * @return this builder
     */
    public Mqtt3ClientBuilder webSocketConfig(plus.jmqx.client.mqtt.internal.transport.MqttWebSocketConfig webSocketConfig) {
        config.setWebSocketConfig(webSocketConfig);
        return this;
    }

    /**
     * 确保客户端标识符已设置，若未设置则生成随机标识符。
     */
    private void ensureClientId() {
        if (config.getClientId() == null || config.getClientId().isEmpty()) {
            config.setClientId("jmqx-" + UUID.randomUUID().toString().substring(0, 8));
        }
        config.setVersion(MqttVersion.MQTT_3_1_1);
    }

    /**
     * 构建 Reactor API 客户端（Mono/Flux）。
     *
     * @return Reactor API 客户端
     */
    public Mqtt3RxClient buildRx() {
        ensureClientId();
        return new DefaultMqtt3Client(config, connected, disconnected);
    }

    /**
     * 构建 CompletableFuture 异步 API 客户端。
     *
     * @return 异步 API 客户端
     */
    public Mqtt3AsyncClient buildAsync() {
        ensureClientId();
        return buildRx().toAsync();
    }

    /**
     * 构建阻塞 API 客户端。
     *
     * @return 阻塞 API 客户端
     */
    public Mqtt3BlockingClient buildBlocking() {
        ensureClientId();
        return buildRx().toBlock();
    }

}
