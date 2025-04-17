package plus.jmqx.broker.cluster;

import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.spi.DynamicLoader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 集群注册中心
 *
 * @author maxid
 * @since 2025/4/9 14:16
 */
public interface ClusterRegistry {
    ClusterRegistry INSTANCE = DynamicLoader.findFirst(ClusterRegistry.class).orElse(null);

    /**
     * 开始监听
     *
     * @param config 集群配置
     */
    void registry(MqttConfiguration.ClusterConfig config);

    /**
     * 开始订阅消息
     *
     * @return {@link ClusterMessage} 集群消息
     */
    Flux<ClusterMessage> handlerClusterMessage();

    /**
     * 开始订阅节点事件
     *
     * @return {@link ClusterStatus} 集群状态
     */
    Flux<ClusterStatus> clusterEvent();

    /**
     * 获取集群节点信息
     *
     * @return {@link ClusterNode} 集群节点
     */
    List<ClusterNode> getClusterNode();

    /**
     * 扩散消息
     *
     * @param clusterMessage 集群消息
     * @return {@link Mono}
     */
    Mono<Void> spreadMessage(ClusterMessage clusterMessage);

    /**
     * 扩散消息
     *
     * @param message Mqtt Publish 消息
     * @return {@link Mono}
     */
    default Mono<Void> spreadPublishMessage(ClusterMessage message) {
        return spreadMessage(message);
    }

    /**
     * 停止
     *
     * @return {@link Mono}
     */
    Mono<Void> shutdown();
}
