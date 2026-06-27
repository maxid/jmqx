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

    /**
     * 获取集群注册中心实例
     *
     * @return 集群注册中心实例
     */
    static ClusterRegistry getInstance() {
        return DynamicLoader.findFirst(ClusterRegistry.class).orElse(null);
    }

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
     * 注册会话与所在节点的映射关系
     *
     * @param clientId 客户端 ID
     */
    default void registerSession(String clientId) {
    }

    /**
     * 移除会话与所在节点的映射关系
     *
     * @param clientId 客户端 ID
     */
    default void unregisterSession(String clientId) {
    }

    /**
     * 注册主题订阅路由
     *
     * @param topicFilter 主题过滤器
     * @param nodeId      节点 ID
     */
    default void subscribeTopic(String topicFilter, String nodeId) {
    }

    /**
     * 移除主题订阅路由
     *
     * @param topicFilter 主题过滤器
     * @param nodeId      节点 ID
     */
    default void unsubscribeTopic(String topicFilter, String nodeId) {
    }

    /**
     * 停止
     *
     * @return 处理结果
     */
    Mono<Void> shutdown();

}
