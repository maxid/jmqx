/**
 * JMQX MQTT 客户端公共 API 根包。
 *
 * <p>通过 {@link plus.jmqx.client.mqtt.MqttClient#builder()} 选择协议版本并构建客户端：
 *
 * <ul>
 *   <li>{@link plus.jmqx.client.mqtt.MqttClientBuilder#useMqttVersion3()} — MQTT 3.1.1</li>
 *   <li>{@link plus.jmqx.client.mqtt.MqttClientBuilder#useMqttVersion5()} — MQTT 5.0</li>
 * </ul>
 *
 * <p>每个版本提供三种编程模型：Reactor（{@code Mono}/{@code Flux}）、
 * {@link java.util.concurrent.CompletableFuture} 异步、以及阻塞 API。
 *
 * @since 1.4.14
 */
package plus.jmqx.client.mqtt;
