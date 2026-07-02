/**
 * MQTT 3.1.1 客户端 API 根包。
 *
 * <p>提供三种编程模型的客户端：Reactive（{@link plus.jmqx.client.mqtt.v3.Mqtt3RxClient}）、
 * 异步（{@link plus.jmqx.client.mqtt.v3.Mqtt3AsyncClient}）和阻塞
 * （{@link plus.jmqx.client.mqtt.v3.Mqtt3BlockingClient}）。
 *
 * <p>入口：{@link plus.jmqx.client.mqtt.MqttClientBuilder#useMqttVersion3()} 或
 * {@link plus.jmqx.client.mqtt.v3.Mqtt3Client#builder()}。
 *
 * @since 1.4.14
 */
package plus.jmqx.client.mqtt.v3;
