package plus.jmqx.client.mqtt.v5;

import plus.jmqx.client.mqtt.MqttGlobalPublishFilter;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5SubAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Unsubscribe;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * MQTT 5 客户端的 reactive API（Reactor Mono/Flux）。
 *
 * @author maxid
 */
public interface Mqtt5RxClient extends Mqtt5Client {

    Mono<Mqtt5ConnAck> connect();

    Mono<Mqtt5SubAck> subscribe(Mqtt5Subscribe subscribe);

    Flux<Mqtt5Publish> subscribePublishes(Mqtt5Subscribe subscribe);

    Flux<Mqtt5Publish> publishes(MqttGlobalPublishFilter filter);

    Mono<Mqtt5PublishResult> publish(Mqtt5Publish publish);

    Mono<Void> unsubscribe(Mqtt5Unsubscribe unsubscribe);

    Mono<Void> disconnect();

    @Override
    default Mqtt5RxClient toRx() {
        return this;
    }
}
