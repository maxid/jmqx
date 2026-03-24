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

    /**
     * 注册集群配置。
     *
     * @param config 集群配置
     */
    @Override
    public void registry(MqttConfiguration.ClusterConfig config) {

    }

    /**
     * 返回空的集群消息流。
     *
     * @return 空消息流
     */
    @Override
    public Flux<ClusterMessage> handlerClusterMessage() {
        return Flux.empty();
    }

    /**
     * 返回空的集群事件流。
     *
     * @return 空事件流
     */
    @Override
    public Flux<ClusterStatus> clusterEvent() {
        return Flux.empty();
    }

    /**
     * 获取集群节点列表。
     *
     * @return 空列表
     */
    @Override
    public List<ClusterNode> getClusterNode() {
        return Collections.emptyList();
    }

    /**
     * 扩散消息的空实现。
     *
     * @param clusterMessage 集群消息
     * @return 空结果
     */
    @Override
    public Mono<Void> spreadMessage(ClusterMessage clusterMessage) {
        return Mono.empty();
    }

    /**
     * 关闭集群的空实现。
     *
     * @return 空结果
     */
    @Override
    public Mono<Void> shutdown() {
        return Mono.empty();
    }

}
