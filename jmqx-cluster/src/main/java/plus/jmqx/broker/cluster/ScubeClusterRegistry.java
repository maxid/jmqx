package plus.jmqx.broker.cluster;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.reactor.RetryNonSerializedEmitFailureHandler;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.message.CloseMqttMessage;
import plus.jmqx.broker.mqtt.message.HeapMqttMessage;
import plus.jmqx.broker.util.PortUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 基于 scube-cluster 实现 MQTT Broker 集群注册中心
 *
 * @author maxid
 * @since 2025/4/22 09:56
 */
@Slf4j
public class ScubeClusterRegistry implements ClusterRegistry {

    private final Sinks.Many<ClusterMessage> messageMany = Sinks.many().multicast().onBackpressureBuffer();

    private final Sinks.Many<ClusterStatus> eventMany = Sinks.many().multicast().onBackpressureBuffer();

    private Cluster cluster;

    /**
     * 本节点在集群中的标识（namespace:node）
     */
    private String localNodeId;

    /**
     * 会话路由表：clientId -> nodeId（托管该会话的节点）
     */
    private final Map<String, String> sessionNodes = new ConcurrentHashMap<>();

    /**
     * 注册集群并启动
     *
     * @param clusterConfig 集群配置
     */
    @Override
    public void registry(MqttConfiguration.ClusterConfig clusterConfig) {
        this.localNodeId = clusterConfig.getClusterId();
        clusterConfig.setPort(PortUtil.getAvailablePort(clusterConfig.getPort()));
        this.cluster = new ClusterImpl()
                .config(opts ->
                        opts.memberAlias(clusterConfig.getNode())
                                .externalHost(Optional.ofNullable(clusterConfig.getExternal())
                                        .map(MqttConfiguration.ClusterExternal::getHost)
                                        .orElse(null))
                                .externalPort(Optional.ofNullable(clusterConfig.getExternal())
                                        .map(MqttConfiguration.ClusterExternal::getPort)
                                        .orElse(null))
                )
                .transportFactory(TcpTransportFactory::new)
                .transport(transportConfig -> transportConfig.port(clusterConfig.getPort()))
                .membership(opts -> opts.seedMembers(Arrays.stream(clusterConfig
                                .getUrl()
                                .split(","))
                                .map(Address::from)
                        .collect(Collectors.toList()))
                        .namespace(clusterConfig.getNamespace())
                        .suspicionMult(clusterConfig.getSuspicionMult() != null
                                ? clusterConfig.getSuspicionMult() : 10))
                .failureDetector(fdet -> fdet.pingTimeout(clusterConfig.getPingTimeout() != null
                        ? clusterConfig.getPingTimeout() : 3000))
                .handler(cluster -> new ClusterHandler())
                .startAwait();
    }

    /**
     * 获取集群消息流
     *
     * @return 集群消息流
     */
    @Override
    public Flux<ClusterMessage> handlerClusterMessage() {
        return messageMany.asFlux();
    }

    /**
     * 获取集群节点列表
     *
     * @return 节点列表
     */
    @Override
    public List<ClusterNode> getClusterNode() {
        return Optional.ofNullable(cluster)
                .map(cs -> cs.members().stream().map(this::clusterNode).collect(Collectors.toList()))
                .orElse(Collections.emptyList());
    }

    /**
     * 转换成员为集群节点
     *
     * @param member 集群成员
     * @return 节点信息
     */
    private ClusterNode clusterNode(Member member) {
        return ClusterNode.builder()
                .alias(member.alias())
                .host(member.address().host())
                .port(member.address().port())
                .namespace(member.namespace())
                .build();
    }

    /**
     * 扩散集群消息
     * <p>
     * 对于定向消息（CLOSE / PUBLISH_TARGET），优先通过会话路由表发送到目标节点，
     * 避免全量广播。未知目标时降级为广播。
     *
     * @param clusterMessage 集群消息
     * @return 处理结果
     */
    @Override
    public Mono<Void> spreadMessage(ClusterMessage clusterMessage) {
        log.debug("cluster send message {} ", clusterMessage);
        if (cluster == null) {
            return Mono.empty();
        }
        // 定向消息：尝试路由到目标节点
        String targetNode = resolveTargetNode(clusterMessage);
        if (targetNode != null) {
            if (targetNode.equals(localNodeId)) {
                // 目标就是本节点，无需发送
                return Mono.empty();
            }
            return sendToMember(targetNode, clusterMessage);
        }
        // 未知目标 / 广播消息：发送到所有其他节点
        return broadcastToAll(clusterMessage);
    }

    /**
     * 解析定向消息的目标节点
     *
     * @param clusterMessage 集群消息
     * @return 目标节点 ID，非定向消息返回 null
     */
    private String resolveTargetNode(ClusterMessage clusterMessage) {
        ClusterMessage.ClusterEvent event = clusterMessage.getClusterEvent();
        if (ClusterMessage.ClusterEvent.CLOSE.equals(event)) {
            Object msg = clusterMessage.getMessage();
            if (msg instanceof CloseMqttMessage closeMsg) {
                return sessionNodes.get(closeMsg.getClientId());
            }
        }
        if (ClusterMessage.ClusterEvent.PUBLISH_TARGET.equals(event)) {
            Object msg = clusterMessage.getMessage();
            if (msg instanceof HeapMqttMessage heapMsg) {
                return sessionNodes.get(heapMsg.getClientId());
            }
        }
        return null;
    }

    /**
     * 发送消息到指定节点
     *
     * @param nodeId         节点 ID（namespace:node）
     * @param clusterMessage 集群消息
     * @return 处理结果
     */
    private Mono<Void> sendToMember(String nodeId, ClusterMessage clusterMessage) {
        return cluster.members().stream()
                .filter(member -> nodeId.equals(member.namespace() + ":" + member.alias()))
                .findFirst()
                .map(member -> cluster.send(member, Message.withData(clusterMessage).build()).then())
                .orElseGet(() -> {
                    log.warn("target node [{}] not found in cluster members, fallback to broadcast", nodeId);
                    return broadcastToAll(clusterMessage);
                });
    }

    /**
     * 广播消息到所有其他节点
     *
     * @param clusterMessage 集群消息
     * @return 处理结果
     */
    private Mono<Void> broadcastToAll(ClusterMessage clusterMessage) {
        return Mono.when(cluster.otherMembers()
                .stream()
                .map(member -> cluster.send(member, Message.withData(clusterMessage).build()).then())
                .collect(Collectors.toList()));
    }

    /**
     * 注册会话路由：记录 clientId 由本节点托管
     *
     * @param clientId 客户端 ID
     */
    @Override
    public void registerSession(String clientId) {
        if (localNodeId != null && clientId != null && !clientId.isEmpty()) {
            sessionNodes.put(clientId, localNodeId);
            log.debug("session route registered: clientId=[{}] -> node=[{}]", clientId, localNodeId);
        }
    }

    /**
     * 移除会话路由
     *
     * @param clientId 客户端 ID
     */
    @Override
    public void unregisterSession(String clientId) {
        if (clientId != null) {
            sessionNodes.remove(clientId);
            log.debug("session route removed: clientId=[{}]", clientId);
        }
    }

    /**
     * 关闭集群
     *
     * @return 处理结果
     */
    @Override
    public Mono<Void> shutdown() {
        return Mono.fromRunnable(() -> {
            sessionNodes.clear();
            Optional.ofNullable(cluster).ifPresent(Cluster::shutdown);
        });
    }

    /**
     * 获取集群事件流
     *
     * @return 集群事件流
     */
    @Override
    public Flux<ClusterStatus> clusterEvent() {
        return eventMany.asFlux();
    }

    class ClusterHandler implements ClusterMessageHandler {

        /**
         * 处理集群消息
         *
         * @param message 集群消息
         */
        @Override
        public void onMessage(Message message) {
            log.debug("cluster accept message {} ", message);
            messageMany.emitNext(message.data(), new RetryNonSerializedEmitFailureHandler());
        }

        /**
         * 处理 Gossip 消息
         *
         * @param message Gossip 消息
         */
        @Override
        public void onGossip(Message message) {
            log.debug("cluster accept gossip message {} ", message);
            messageMany.emitNext(message.data(), new RetryNonSerializedEmitFailureHandler());
        }

        /**
         * 处理成员事件
         *
         * @param event 成员事件
         */
        @Override
        public void onMembershipEvent(MembershipEvent event) {
            Member member = event.member();
            log.info("cluster onMembershipEvent {}  {}", member, event);
            switch (event.type()) {
                case ADDED:
                    eventMany.tryEmitNext(ClusterStatus.ADDED);
                    break;
                case LEAVING:
                    eventMany.tryEmitNext(ClusterStatus.LEAVING);
                    break;
                case REMOVED:
                    // 节点离开时清除其托管的会话路由
                    removeSessionRouteByNode(member.namespace() + ":" + member.alias());
                    eventMany.tryEmitNext(ClusterStatus.REMOVED);
                    break;
                case UPDATED:
                    eventMany.tryEmitNext(ClusterStatus.UPDATED);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 移除指定节点托管的所有会话路由
     *
     * @param nodeId 节点 ID
     */
    private void removeSessionRouteByNode(String nodeId) {
        int size = sessionNodes.size();
        sessionNodes.values().removeIf(nodeId::equals);
        int removed = size - sessionNodes.size();
        if (removed > 0) {
            log.info("cleared {} session routes for leaving node [{}]", removed, nodeId);
        }
    }

}
