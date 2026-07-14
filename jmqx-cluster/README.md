# jmqx-cluster — MQTT Broker 集群模块

基于 [ScaleCube](https://github.com/scalecube/scalecube) 实现 MQTT Broker 集群，支持多节点水平扩展与消息路由。

## 功能特性

- **节点自动发现**：通过种子节点列表自动组建集群
- **消息路由扩散**：PUBLISH 消息自动扩散到集群中其他节点
- **订阅感知路由（1.4.13+）**：只有订阅匹配的节点才会收到 PUBLISH 扩散，大幅减少无效网络传输
- **会话路由**：维护 clientId → 节点映射，支持定向消息投递
- **故障检测**：基于 gossip 协议的故障检测器，自动发现节点上下线
- **主机路由清理**：节点离开时自动清理其所托管的会话和主题路由
- **集成测试支持**：同一 JVM 内可通过 `namespace + node` 启动多个集群节点
- **前缀索引加速**：主题匹配使用前缀分桶 + LRU 缓存，优化高频匹配场景

## 架构概览

```
┌───────────────────┐      ┌───────────────────┐
│   jmqx-cluster    │      │   jmqx-cluster    │
│   node-1:7771     │      │   node-2:7772     │
│                   │      │                   │
│  ┌───────────┐    │      │  ┌───────────┐    │
│  │ Session   │◄───┼──────┼─►│ Session   │    │
│  │ Route     │    │      │  │ Route     │    │
│  └───────────┘    │      │  └───────────┘    │
│  ┌───────────┐    │      │  ┌───────────┐    │
│  │ Topic     │◄───┼──────┼─►│ Topic     │    │
│  │ Route     │    │gossip│  │ Route     │    │
│  └───────────┘    │      │  └───────────┘    │
│  ┌───────────┐    │      │  ┌───────────┐    │
│  │ MQTT:1883 │    │      │  │ MQTT:2883 │    │
│  └───────────┘    │      │  └───────────┘    │
└───────────────────┘      └───────────────────┘
```

## 依赖

- `jmqx-broker` — Broker 核心库
- `scalecube-cluster` — 集群通信框架
- `scalecube-transport-netty` — 基于 Netty 的集群传输层

## 快速开始

```xml
<dependency>
    <groupId>plus.jmqx.iot</groupId>
    <artifactId>jmqx-cluster</artifactId>
    <version>1.4.17</version>
</dependency>
```

双节点集群示例：

```java
// node-1
MqttConfiguration config = new MqttConfiguration();
config.getClusterConfig().setEnable(true);
config.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
config.getClusterConfig().setPort(7771);
config.getClusterConfig().setNode("node-1");
config.getClusterConfig().setNamespace("jmqx");
new Bootstrap(config).startAwait();

// node-2（配置不同端口和节点名）
MqttConfiguration config2 = new MqttConfiguration();
config2.setPort(2883);
config2.getClusterConfig().setEnable(true);
config2.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
config2.getClusterConfig().setPort(7772);
config2.getClusterConfig().setNode("node-2");
config2.getClusterConfig().setNamespace("jmqx");
new Bootstrap(config2).startAwait();
```

## 集群配置

| 参数 | 说明 | 默认值 |
|---|---|---|
| `cluster.enable` | 是否启用集群 | `false` |
| `cluster.url` | 种子节点地址列表（逗号分隔） | — |
| `cluster.port` | 集群通信端口 | `7771` |
| `cluster.node` | 本节点名称（需唯一） | `node-1` |
| `cluster.namespace` | 集群命名空间（需一致才能通信） | `jmqx-broker` |
| `cluster.suspicionMult` | 怀疑倍数 | `10` |
| `cluster.pingTimeout` | 故障检测器超时（毫秒） | `3000` |
| `cluster.clusterMessageBufferSize` | 集群消息缓冲区大小 | `1024` |
| `cluster.external.host` | 容器/云环境外部暴露 IP | — |
| `cluster.external.port` | 容器/云环境外部端口 | — |

## 关键实现

- `ScubeClusterRegistry`：基于 ScaleCube 的 `ClusterRegistry` SPI 实现
- `JacksonMessageCodec`：使用 Jackson 进行集群消息的序列化/反序列化
- 前缀索引 + LRU 匹配缓存：主题路由使用前缀分桶加速（`extractPrefix`），匹配结果使用 512 条目 LRU 缓存
- 故障自动清理：节点离开集群时，自动清理该节点的会话路由和主题路由