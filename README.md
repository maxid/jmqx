# Jmqx 开源的轻量级 MQTT 消息代理 Broker

Jmqx lightweight MQTT Broker

Jmqx 是在 [SMQTT 1.x](https://github.com/quickmsg/smqtt) 基础上的重构版本，主要是对 MQTT 实现部分的重构，并修正了一些问题，SMQTT 是作为一个独立的完整应用，而 Jmqx 的目标是作为一个库栈嵌入到用户应用中，为用户应用提供 MQTT 设备接入能力，实现自己的物联网平台，在此感谢 SMQTT 作者开源了那么优秀的项目。

## 相同点：
1. 支持 MQTT v3.1、v3.1.1、v5 协议（v5不完整实现）
2. 支持集群

## 不同点：
1. 不提供消息持久化（只提供内存实现，若需要可参考 SMQTT 或 SMQTTX 的实现）
2. 不提供规则引擎
3. 不提供读取配置文件（参考 jmqx-example 注入配置）

## 改进点：
1. 可同时支持 MQTT、MQTTS、MQTT-WS、MQTT-WSS 端口监听，方便不同需求的设备接入
2. 提供构造器注入用户自定义设备鉴权管理、主题访问控制管理、设备生命周期监听模块，方便与spring boot等框架集成
3. 方便作为库栈嵌入用户应用（参考 jmqx-example）
4. 修复了订阅消息QoS、Retained状态错误、Retain消息重发、消息顺序异常等问题
5. 同一进程内可以通过设置不同命名空间启动多个实例（2026-1-11）
6. 增加 PortUtil 在端口占用时自动获取新端口（2026-1-14）

## 使用示例
- 单元测试方式启动：单节点

引入依赖
```xml
        <dependency>
            <groupId>plus.jmqx.iot</groupId>
            <artifactId>jmqx-broker</artifactId>
            <version>1.4.5</version>
        </dependency>
```
编写测试用例
```java
/**
 * MQTT Broker 测试用例
 */
@Slf4j
class BootstrapTest {
    @Test
    void brokerTest() throws Exception {
        // 日志配置（可选）
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.mqtt.message.impl").setLevel(Level.DEBUG);
        // 构建配置信息
        MqttConfiguration config = new MqttConfiguration();
        // 设置启用SSL（可选）
        config.setSslEnable(true);
        config.setSslCa(Objects.requireNonNull(BootstrapTest.class.getResource("/ca.crt")).getPath());
        config.setSslCrt(Objects.requireNonNull(BootstrapTest.class.getResource("/server.crt")).getPath());
        config.setSslKey(Objects.requireNonNull(BootstrapTest.class.getResource("/server.key")).getPath());
        // 构建器注入配置信息（必选）及设备生命周期订阅器（可选）
        Bootstrap bootstrap = new Bootstrap(config, new PlatformDispatcher() {
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("设备上线：{}", message);
                });
            }

            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("设备断开连接：{}", message);
                });
            }

            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("设备离线：{}", message);
                });
            }

            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("设备上报消息：PublishMessage(clientId={}, username={}, topic={}, payload={})",
                            message.getClientId(),
                            message.getUsername(),
                            message.getTopic(),
                            new String(message.getPayload(), StandardCharsets.UTF_8));
                });
            }
        });
        bootstrap.start().block();
        Thread.sleep(3600 * 1000);
        bootstrap.shutdown();
    }
}
```
启动测试用例控制台输出
```shell
14:13:53.734 [jmqx-event-loop-select-nio-2] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt broker start success host 0:0:0:0:0:0:0:0 port 1883
14:13:53.901 [jmqx-event-loop-select-nio-3] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtts broker start success host 0:0:0:0:0:0:0:0 port 8883
14:13:53.902 [jmqx-event-loop-select-nio-4] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt-ws broker start success host 0:0:0:0:0:0:0:0 port 1884
14:13:53.906 [jmqx-event-loop-select-nio-5] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt-wss broker start success host 0:0:0:0:0:0:0:0 port 8884
```
- 单元测试方式启动：本机集群

引入依赖
```xml
        <dependency>
            <groupId>plus.jmqx.iot</groupId>
            <artifactId>jmqx-cluster</artifactId>
            <version>1.4.5</version>
        </dependency>
```
编写测试用例
```java
/**
 * 集群测试用例
 */
@Slf4j
public class BootstrapTest {
    @Test
    void cluster01() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.cluster").setLevel(Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        MqttConfiguration config = new MqttConfiguration();
        config.getClusterConfig().setEnable(true);
        config.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
        config.getClusterConfig().setPort(7771);
        config.getClusterConfig().setNode("node-1");
        config.getClusterConfig().setNamespace("jmqx");
        Bootstrap bootstrap = new Bootstrap(config);
        bootstrap.start().block();
        Thread.sleep(3600 * 1000);
        bootstrap.shutdown();
    }

    @Test
    void cluster02() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.cluster").setLevel(Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        MqttConfiguration config = new MqttConfiguration();
        config.setPort(2883);
        config.setSecurePort(2884);
        config.setWebsocketPort(9883);
        config.setWebsocketSecurePort(9884);
        config.getClusterConfig().setEnable(true);
        config.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
        config.getClusterConfig().setPort(7772);
        config.getClusterConfig().setNode("node-2");
        config.getClusterConfig().setNamespace("jmqx");
        Bootstrap bootstrap = new Bootstrap(config);
        bootstrap.start().block();
        Thread.sleep(3600 * 1000);
        bootstrap.shutdown();
    }
}
```
启动测试用例控制台输出
```shell
14:21:52.411 [main] INFO io.scalecube.cluster.Cluster - [null][doStart] Starting, config: ClusterConfig[metadata=null, metadataTimeout=3000, metadataCodec=io.scalecube.cluster.metadata.JdkMetadataCodec@57ad2aa7, memberId='null', memberAlias='node-1', externalHost='null', externalPort=null, transportConfig=TransportConfig[port=7771, clientSecured=false, connectTimeout=3000, messageCodec=plus.jmqx.broker.cluster.JacksonMessageCodec@5b3f61ff, maxFrameLength=2097152, transportFactory=io.scalecube.transport.netty.tcp.TcpTransportFactory@3e2059ae, addressMapper=java.util.function.Function$$Lambda$1/1560911714@398dada8], failureDetectorConfig=FailureDetectorConfig[pingInterval=1000, pingTimeout=500, pingReqMembers=3], gossipConfig=GossipConfig[gossipFanout=3, gossipInterval=200, gossipRepeatMult=3, gossipSegmentationThreshold=1000], membershipConfig=MembershipConfig[seedMembers=[127.0.0.1:7771, 127.0.0.1:7772], syncInterval=30000, syncTimeout=3000, suspicionMult=5, namespace='jmqx', removedMembersHistorySize=42]]
14:21:52.518 [sc-cluster-io-nio-1] INFO io.scalecube.cluster.transport.api.Transport - [start][/0:0:0:0:0:0:0:0:7771] Bound cluster transport
14:21:52.536 [sc-cluster-io-nio-1] WARN io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771] Filtering out seed address: 127.0.0.1:7771
14:21:52.540 [sc-cluster-io-nio-1] INFO io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771] Making initial Sync to all seed members: [127.0.0.1:7772]
14:21:52.619 [sc-cluster-io-nio-1] WARN io.scalecube.cluster.transport.api.Transport - [127.0.0.1:7771][connect][error] remoteAddress: 127.0.0.1:7772, cause: io.netty.channel.AbstractChannel$AnnotatedConnectException: Connection refused: /127.0.0.1:7772
14:21:52.619 [sc-cluster-io-nio-1] WARN io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771] Exception on initial Sync, cause: io.netty.channel.AbstractChannel$AnnotatedConnectException: Connection refused: /127.0.0.1:7772
14:21:52.620 [sc-cluster-7771-1] INFO io.scalecube.cluster.Cluster - [jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771][doStart] Started
14:21:52.625 [jmqx-event-loop-select-nio-2] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt broker start success host 0:0:0:0:0:0:0:0 port 1883
14:21:52.626 [jmqx-event-loop-select-nio-3] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt-ws broker start success host 0:0:0:0:0:0:0:0 port 1884
```
```shell
14:22:39.104 [main] INFO io.scalecube.cluster.Cluster - [null][doStart] Starting, config: ClusterConfig[metadata=null, metadataTimeout=3000, metadataCodec=io.scalecube.cluster.metadata.JdkMetadataCodec@57ad2aa7, memberId='null', memberAlias='node-2', externalHost='null', externalPort=null, transportConfig=TransportConfig[port=7772, clientSecured=false, connectTimeout=3000, messageCodec=plus.jmqx.broker.cluster.JacksonMessageCodec@5b3f61ff, maxFrameLength=2097152, transportFactory=io.scalecube.transport.netty.tcp.TcpTransportFactory@3e2059ae, addressMapper=java.util.function.Function$$Lambda$1/1560911714@398dada8], failureDetectorConfig=FailureDetectorConfig[pingInterval=1000, pingTimeout=500, pingReqMembers=3], gossipConfig=GossipConfig[gossipFanout=3, gossipInterval=200, gossipRepeatMult=3, gossipSegmentationThreshold=1000], membershipConfig=MembershipConfig[seedMembers=[127.0.0.1:7771, 127.0.0.1:7772], syncInterval=30000, syncTimeout=3000, suspicionMult=5, namespace='jmqx', removedMembersHistorySize=42]]
14:22:39.212 [sc-cluster-io-nio-1] INFO io.scalecube.cluster.transport.api.Transport - [start][/0:0:0:0:0:0:0:0:7772] Bound cluster transport
14:22:39.228 [sc-cluster-io-nio-1] WARN io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-2:259b35b190434e83@127.0.0.1:7772] Filtering out seed address: 127.0.0.1:7772
14:22:39.232 [sc-cluster-io-nio-1] INFO io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-2:259b35b190434e83@127.0.0.1:7772] Making initial Sync to all seed members: [127.0.0.1:7771]
14:22:39.511 [sc-cluster-7772-1] INFO io.scalecube.cluster.membership.MembershipProtocol - [jmqx:node-2:259b35b190434e83@127.0.0.1:7772][publishEvent] MembershipEvent[type=ADDED, member=jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771, oldMetadata=null, newMetadata=7e16449-5, timestamp=2025-04-23T06:22:39.510Z]
14:22:39.514 [sc-cluster-7772-1] INFO plus.jmqx.broker.cluster.ScubeClusterRegistry - cluster onMembershipEvent jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771  MembershipEvent[type=ADDED, member=jmqx:node-1:c2bb3af63614e2a@127.0.0.1:7771, oldMetadata=null, newMetadata=7e16449-5, timestamp=2025-04-23T06:22:39.510Z]
14:22:39.515 [sc-cluster-7772-1] INFO io.scalecube.cluster.Cluster - [jmqx:node-2:259b35b190434e83@127.0.0.1:7772][doStart] Started
14:22:39.518 [jmqx-event-loop-select-nio-2] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt broker start success host 0:0:0:0:0:0:0:0 port 2883
14:22:39.519 [jmqx-event-loop-select-nio-3] INFO plus.jmqx.broker.mqtt.transport.impl.MqttTransport - mqtt-ws broker start success host 0:0:0:0:0:0:0:0 port 2884
```

## 生成证书
> 这理采用[generate-CA.sh](https://github.com/owntracks/tools/blob/master/TLS/generate-CA.sh)来生成双向认证自签证书

从 https://github.com/owntracks/tools/blob/master/TLS/generate-CA.sh 下载证书生成脚本
```shell
# 生成服务端证书
./generate-CA.sh server
# 生成客户端证书
./generate-CA.sh client client
```