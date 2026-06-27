package plus.jmqx.broker.cluster;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.metrics.MetricsManagerHolder;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.message.CloseMqttMessage;
import plus.jmqx.broker.mqtt.message.HeapMqttMessage;
import plus.jmqx.broker.util.PortUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    /**
     * 集群消息管道
     * <p>
     * 延迟初始化，在 registry() 中创建可配置大小的 Sink。
     */
    private Sinks.Many<ClusterMessage> messageMany;

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
     * 主题路由表：topicFilter -> 有订阅者的节点 ID 集合
     */
    private final Map<String, Set<String>> topicNodes = new ConcurrentHashMap<>();

    /**
     * 主题前缀索引：前缀 -> topicFilter 集合
     * <p>
     * 加速 resolvePublishTargets 的查找，将 O(N) 扫描降为 O(bucket)。
     * </p>
     */
    private final Map<String, Set<String>> prefixIndex = new ConcurrentHashMap<>();

    /**
     * topicMatches 结果缓存（前缀分桶 + LRU）
     */
    private final Map<String, Map<String, Boolean>> matchCache = new ConcurrentHashMap<>();

    private static final int MATCH_CACHE_ENTRY = 512;

    /**
     * 注册集群并启动
     *
     * @param clusterConfig 集群配置
     */
    @Override
    public void registry(MqttConfiguration.ClusterConfig clusterConfig) {
        this.localNodeId = clusterConfig.getClusterId();
        clusterConfig.setPort(PortUtil.getAvailablePort(clusterConfig.getPort()));
        // 使用配置的缓冲区大小创建 Sink
        int bufferSize = clusterConfig.getClusterMessageBufferSize() != null
                ? clusterConfig.getClusterMessageBufferSize() : 1024;
        this.messageMany = Sinks.many().multicast().onBackpressureBuffer(bufferSize, false);
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
                .failureDetector(fdet -> fdet.pingInterval(2000)
                        .pingTimeout(clusterConfig.getPingTimeout() != null
                                ? clusterConfig.getPingTimeout() : 5000))
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
     * PUBLISH 事件：根据主题路由表过滤，只发给有匹配订阅的节点。
     * CLOSE / PUBLISH_TARGET：通过会话路由表发送到目标节点。
     * 未知目标降级为全量广播。
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
                return Mono.empty();
            }
            return sendToMember(targetNode, clusterMessage);
        }
        // PUBLISH 事件：根据主题路由过滤
        if (ClusterMessage.ClusterEvent.PUBLISH.equals(clusterMessage.getClusterEvent())) {
            return spreadPublish(clusterMessage);
        }
        // 其他事件（SUBSCRIBE 等）：全量广播
        return broadcastToAll(clusterMessage);
    }

    /**
     * 按主题路由扩散 PUBLISH 消息，只发给有匹配订阅者的节点
     *
     * @param clusterMessage 集群消息
     * @return 处理结果
     */
    private Mono<Void> spreadPublish(ClusterMessage clusterMessage) {
        Object msg = clusterMessage.getMessage();
        if (!(msg instanceof HeapMqttMessage heapMsg)) {
            return broadcastToAll(clusterMessage);
        }
        String topic = heapMsg.getTopic();
        // 收集有匹配订阅的远程节点
        Set<String> targets = resolvePublishTargets(topic);
        if (targets.isEmpty()) {
            // 没有任何节点有匹配订阅，跳过发送
            return Mono.empty();
        }
        // 只发给有匹配订阅的节点
        return Mono.when(cluster.otherMembers().stream()
                .filter(member -> {
                    String nodeId = member.namespace() + ":" + member.alias();
                    return targets.contains(nodeId);
                })
                .map(member -> cluster.send(member, Message.withData(clusterMessage).build()).then())
                .collect(Collectors.toList()));
    }

    /**
     * 提取主题/过滤器第一段作为前缀
     */
    private static String extractPrefix(String filter) {
        if (filter == null || filter.isEmpty()) {
            return "";
        }
        int idx = filter.indexOf('/');
        return idx < 0 ? filter : (idx > 0 ? filter.substring(0, idx) : "/");
    }

    /**
     * 解析主题的订阅目标节点（使用前缀索引加速）
     *
     * @param topic 发布主题
     * @return 有匹配订阅的节点 ID 集合
     */
    private Set<String> resolvePublishTargets(String topic) {
        String prefix = extractPrefix(topic);
        Set<String> candidates = new HashSet<>();
        // 1. 取该前缀直接命中的 filter
        Set<String> direct = prefixIndex.get(prefix);
        if (direct != null) candidates.addAll(direct);
        // 2. 通配符前缀也可能匹配任何主题
        Set<String> multi = prefixIndex.get("#");
        if (multi != null) candidates.addAll(multi);
        Set<String> single = prefixIndex.get("+");
        if (single != null) candidates.addAll(single);
        Set<String> any = prefixIndex.get("");
        if (any != null) candidates.addAll(any);
        // 3. 精确匹配 topic 的 filter 肯定匹配（无需通配符校验）
        // 注意：topicNodes.get(topic) 返回的是路由表内部 Set 引用，
        // 必须复制后再排除本节点，避免污染全局路由表。
        Set<String> exactNodes = topicNodes.get(topic);
        Set<String> remoteExact = null;
        if (exactNodes != null) {
            remoteExact = new HashSet<>(exactNodes);
            remoteExact.remove(localNodeId);
            // 把精确匹配的 topic 从 candidates 中移除，以免重复验证
            candidates.remove(topic);
        }
        // 4. 逐一验证候选 filter
        Set<String> targets = remoteExact != null && !remoteExact.isEmpty()
                ? new HashSet<>(remoteExact) : new HashSet<>();
        for (String filter : candidates) {
            Set<String> nodes = topicNodes.get(filter);
            if (nodes != null && topicMatchesWithCache(filter, topic)) {
                targets.addAll(nodes);
            }
        }
        targets.remove(localNodeId);
        return targets;
    }

    /**
     * topicMatches 结果缓存版
     */
    private boolean topicMatchesWithCache(String filter, String topic) {
        String bucketKey = extractPrefix(filter);
        Map<String, Boolean> bucket = matchCache.computeIfAbsent(bucketKey,
                k -> Collections.synchronizedMap(new LinkedHashMap<String, Boolean>(64, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                        return size() > MATCH_CACHE_ENTRY;
                    }
                }));
        return bucket.computeIfAbsent(topic, t -> matchFilter(filter, t));
    }

    /**
     * 判断主题过滤器是否匹配实际主题（支持 MQTT 通配符 + 和 #）
     *
     * @param filter 主题过滤器
     * @param topic  实际主题
     * @return 是否匹配
     */
    static boolean matchFilter(String filter, String topic) {
        if (filter == null || topic == null) {
            return false;
        }
        if (filter.equals(topic)) {
            return true;
        }
        if (filter.equals("#")) {
            return true;
        }
        if (filter.endsWith("/#")) {
            String prefix = filter.substring(0, filter.length() - 2);
            if (topic.equals(prefix) || topic.startsWith(prefix + "/")) {
                return true;
            }
        }
        String[] f = filter.split("/");
        String[] t = topic.split("/");
        if (f.length != t.length) {
            return false;
        }
        for (int i = 0; i < f.length; i++) {
            if (!f[i].equals("+") && !f[i].equals(t[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * 注册主题订阅路由
     *
     * @param topicFilter 主题过滤器
     * @param nodeId      节点 ID
     */
    @Override
    public void subscribeTopic(String topicFilter, String nodeId) {
        if (topicFilter == null || nodeId == null) {
            return;
        }
        topicNodes.computeIfAbsent(topicFilter, k -> ConcurrentHashMap.newKeySet()).add(nodeId);
        String prefix = extractPrefix(topicFilter);
        prefixIndex.computeIfAbsent(prefix, k -> ConcurrentHashMap.newKeySet()).add(topicFilter);
        // 清除匹配缓存
        matchCache.remove(extractPrefix(topicFilter));
        log.debug("topic route added: topic=[{}] -> node=[{}]", topicFilter, nodeId);
    }

    /**
     * 移除主题订阅路由
     *
     * @param topicFilter 主题过滤器
     * @param nodeId      节点 ID
     */
    @Override
    public void unsubscribeTopic(String topicFilter, String nodeId) {
        if (topicFilter == null || nodeId == null) {
            return;
        }
        topicNodes.computeIfPresent(topicFilter, (k, v) -> {
            v.remove(nodeId);
            return v.isEmpty() ? null : v;
        });
        // 清理 prefixIndex：topicFilter 不再被任何节点订阅时移除
        String prefix = extractPrefix(topicFilter);
        Set<String> filters = prefixIndex.get(prefix);
        if (filters != null) {
            filters.remove(topicFilter);
            if (filters.isEmpty()) {
                prefixIndex.remove(prefix);
            }
        }
        matchCache.remove(prefix);
        log.debug("topic route removed: topic=[{}] -> node=[{}]", topicFilter, nodeId);
    }

    /**
     * 移除指定节点托管的所有主题路由
     *
     * @param nodeId 节点 ID
     */
    private void removeTopicRouteByNode(String nodeId) {
        int[] count = {0};
        topicNodes.values().forEach(nodes -> {
            if (nodes.remove(nodeId)) {
                count[0]++;
            }
        });
        topicNodes.entrySet().removeIf(e -> e.getValue().isEmpty());
        // 重建 prefixIndex（收集仍然有效的 topicFilter）
        prefixIndex.clear();
        topicNodes.keySet().forEach(filter ->
                prefixIndex.computeIfAbsent(extractPrefix(filter), k -> ConcurrentHashMap.newKeySet()).add(filter));
        matchCache.clear();
        if (count[0] > 0) {
            log.info("cleared {} topic routes for leaving node [{}]", count[0], nodeId);
        }
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
            topicNodes.clear();
            prefixIndex.clear();
            matchCache.clear();
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
            Sinks.EmitResult result = messageMany.tryEmitNext(message.data());
            if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
                log.warn("cluster message sink overflow, dropping from {}", message.sender());
                MetricsManagerHolder.get().recordOverflow("cluster-message");
            }
        }

        /**
         * 处理 Gossip 消息
         *
         * @param message Gossip 消息
         */
        @Override
        public void onGossip(Message message) {
            log.debug("cluster accept gossip message {} ", message);
            Sinks.EmitResult result = messageMany.tryEmitNext(message.data());
            if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
                log.warn("cluster gossip sink overflow, dropping from {}", message.sender());
                MetricsManagerHolder.get().recordOverflow("cluster-gossip");
            }
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
            String nodeId = member.namespace() + ":" + member.alias();
            switch (event.type()) {
                case ADDED:
                    eventMany.tryEmitNext(ClusterStatus.ADDED);
                    break;
                case LEAVING:
                    eventMany.tryEmitNext(ClusterStatus.LEAVING);
                    break;
                case REMOVED:
                    // 节点离开时清除其托管的会话和主题路由
                    removeSessionRouteByNode(nodeId);
                    removeTopicRouteByNode(nodeId);
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
