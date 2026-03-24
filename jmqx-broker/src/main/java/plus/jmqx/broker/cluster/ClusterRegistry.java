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
     * @return 集群消息流
     */
    Flux<ClusterMessage> handlerClusterMessage();

    /**
     * 开始订阅节点事件
     *
     * @return 集群状态流
     */
    Flux<ClusterStatus> clusterEvent();

    /**
     * 获取集群节点信息
     *
     * @return 集群节点列表
     */
    List<ClusterNode> getClusterNode();

    /**
     * 扩散消息
     *
     * @param clusterMessage 集群消息
     * @return 处理结果
     */
    Mono<Void> spreadMessage(ClusterMessage clusterMessage);

    /**
     * 扩散消息
     *
     * @param message Mqtt Publish 消息
     * @return 处理结果
     */
    default Mono<Void> spreadPublishMessage(ClusterMessage message) {
        return spreadMessage(message);
    }

    /**
     * 停止
     *
     * @return 处理结果
     */
    Mono<Void> shutdown();

}
