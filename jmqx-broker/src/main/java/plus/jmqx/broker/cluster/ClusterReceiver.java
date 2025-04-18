package plus.jmqx.broker.cluster;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.cluster.impl.DefaultClusterRegistry;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.context.MqttReceiveContext;
import plus.jmqx.broker.mqtt.message.*;
import plus.jmqx.broker.mqtt.util.JacksonUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

/**
 * 集群服务
 *
 * @author maxid
 * @since 2025/4/17 10:13
 */
@Slf4j
public class ClusterReceiver {
    private final MqttReceiveContext context;

    public ClusterReceiver(MqttReceiveContext context) {
        this.context = context;
    }

    public void registry() {
        MqttConfiguration.ClusterConfig config = context.getConfiguration().getClusterConfig();
        ClusterRegistry cluster = context.getClusterRegistry();
        MessageAdapter messageAdapter = context.getMessageAdapter();
        if (config.isEnable()) {
            if (cluster instanceof DefaultClusterRegistry) {
                Flux.interval(Duration.ofSeconds(5))
                        .subscribe(index -> log.debug("Using fake cluster mode base on DefaultClusterRegistry."));
            } else {
                // 注册集群
                cluster.registry(config);
                // 监听集群消息
                cluster.handlerClusterMessage()
                        .doOnError(err -> log.error("cluster accept", err))
                        .onErrorResume(err -> Mono.empty())
                        .subscribe(message -> {
                            if (ClusterMessage.ClusterEvent.PUBLISH.equals(message.getClusterEvent())) {
                                HeapMqttMessage heapMqttMessage = (HeapMqttMessage) message.getMessage();
                                messageAdapter.dispatch(ClusterSession.wrapClientId(heapMqttMessage.getClientId()),
                                        getMqttMessage(heapMqttMessage),
                                        context);
                            } else {
                                CloseMqttMessage closeMqttMessage = (CloseMqttMessage) message.getMessage();
                                Optional.ofNullable(context.getSessionRegistry().get(closeMqttMessage.getClientId()))
                                        .ifPresent(session -> session.close().subscribe());
                            }
                        });
            }
        }
    }

    private MessageWrapper<MqttMessage> getMqttMessage(HeapMqttMessage heapMqttMessage) {
        return new MessageWrapper<>(MqttMessageBuilder.publishMessage(false,
                MqttQoS.valueOf(heapMqttMessage.getQos()),
                0,
                heapMqttMessage.getTopic(),
                PooledByteBufAllocator.DEFAULT.buffer().writeBytes(
                        JacksonUtil.dynamicJson(heapMqttMessage.getMessage()).getBytes(StandardCharsets.UTF_8)
                ),
                heapMqttMessage.getProperties()),
                System.currentTimeMillis(),
                Boolean.TRUE
        );
    }
}
