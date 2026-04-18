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
import plus.jmqx.broker.util.PortUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
     * 注册集群并启动
     *
     * @param clusterConfig 集群配置
     */
    @Override
    public void registry(MqttConfiguration.ClusterConfig clusterConfig) {
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
                        .collect(Collectors.toList())).namespace(clusterConfig.getNamespace()))
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
     *
     * @param clusterMessage 集群消息
     * @return 处理结果
     */
    @Override
    public Mono<Void> spreadMessage(ClusterMessage clusterMessage) {
        log.debug("cluster send message {} ", clusterMessage);
        return Mono.when(cluster.otherMembers()
                .stream()
                .map(member -> Optional.ofNullable(cluster)
                        .map(c -> c.send(member, Message.withData(clusterMessage).build()).then())
                        .orElse(Mono.empty()))
                .collect(Collectors.toList()));
    }

    /**
     * 关闭集群
     *
     * @return 处理结果
     */
    @Override
    public Mono<Void> shutdown() {
        return Mono.fromRunnable(() -> Optional.ofNullable(cluster)
                .ifPresent(Cluster::shutdown));
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

}
