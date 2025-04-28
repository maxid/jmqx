package plus.jmqx.example.broker.dispatch;

import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class PlatformMessageDispatcher implements PlatformDispatcher {

    private final ConnectionProcessor     connectionProcessor;
    private final DisconnectionProcessor  disconnectionProcessor;
    private final PublishMessageProcessor publishMessageProcessor;

    @Override
    public Mono<Void> onConnect(ConnectMessage message) {
        return Mono.fromRunnable(() -> connectionProcessor.process(message));
    }

    @Override
    public Mono<Void> onDisconnect(DisconnectMessage message) {
        return Mono.fromRunnable(() -> {
        });
    }

    @Override
    public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
        return Mono.fromRunnable(() -> disconnectionProcessor.process(message));
    }

    @Override
    public Mono<Void> onPublish(PublishMessage message) {
        return Mono.fromRunnable(() -> publishMessageProcessor.process(message));
    }
}
