package plus.jmqx.example.broker.dispatch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import plus.jmqx.broker.mqtt.message.dispatch.*;
import reactor.core.publisher.Mono;

/**
 * MQTT消息分发
 *
 * @author maxid
 * @since 2025/4/26 10:51
 */
@Slf4j
@Component
public class PlatformMessageDispatcher implements PlatformDispatcher {
    @Override
    public Mono<Void> onConnect(ConnectMessage message) {
        return Mono.fromRunnable(() -> {});
    }

    @Override
    public Mono<Void> onDisconnect(DisconnectMessage message) {
        return Mono.fromRunnable(() -> {});
    }

    @Override
    public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
        return Mono.fromRunnable(() -> {});
    }

    @Override
    public Mono<Void> onPublish(PublishMessage message) {
        return Mono.fromRunnable(() -> {});
    }
}
