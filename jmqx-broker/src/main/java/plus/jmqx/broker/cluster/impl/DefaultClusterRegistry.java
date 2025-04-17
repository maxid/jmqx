package plus.jmqx.broker.cluster.impl;

import plus.jmqx.broker.cluster.ClusterMessage;
import plus.jmqx.broker.cluster.ClusterNode;
import plus.jmqx.broker.cluster.ClusterRegistry;
import plus.jmqx.broker.cluster.ClusterStatus;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;

/**
 * 空集群实现
 *
 * @author maxid
 * @since 2025/4/17 09:48
 */
public class DefaultClusterRegistry implements ClusterRegistry {
    @Override
    public void registry(MqttConfiguration.ClusterConfig config) {

    }

    @Override
    public Flux<ClusterMessage> handlerClusterMessage() {
        return Flux.empty();
    }

    @Override
    public Flux<ClusterStatus> clusterEvent() {
        return Flux.empty();
    }

    @Override
    public List<ClusterNode> getClusterNode() {
        return Collections.emptyList();
    }

    @Override
    public Mono<Void> spreadMessage(ClusterMessage clusterMessage) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> shutdown() {
        return Mono.empty();
    }
}
