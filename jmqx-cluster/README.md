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
    <version>1.4.18</version>
</dependency>
```

双节点集群示例：

```java
// @formatter:off
// node-1
MqttConfiguration config = new MqttConfiguration();
config.getClusterConfig().setEnabled(true);
config.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
config.getClusterConfig().setPort(7771);
config.getClusterConfig().setNode("node-1");
config.getClusterConfig().setNamespace("jmqx");
new Bootstrap(config).startAwait();

// node-2（配置不同 MQTT 端口和节点名）
MqttConfiguration config2 = new MqttConfiguration();
config2.setPort(2883);
config2.getClusterConfig().setEnabled(true);
config2.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
config2.getClusterConfig().setPort(7772);
config2.getClusterConfig().setNode("node-2");
config2.getClusterConfig().setNamespace("jmqx");
new Bootstrap(config2).startAwait();
// @formatter:on
```

## 集群配置

配置入口分两类：

1. **`MqttConfiguration.ClusterConfig`**（JSON/YAML 中常写作 `cluster`）— ScaleCube 成员、种子、故障检测等  
2. **Broker 线程池**（`MqttConfiguration` 顶层 / `jmqx.tcp.*`）— 集群消息扩散所用 `jmqx-cluster` 调度器  

Spring 示例属性前缀：`jmqx.cluster.*`（成员）与 `jmqx.tcp.cluster-*-*`（扩散线程池）。

### ScaleCube / 成员配置

| 字段（Java） | Spring 示例属性 | 说明 | 默认值 |
|---|---|---|---|
| `enabled` | `jmqx.cluster.enable` | 是否启用集群 | `false` |
| `url` | `jmqx.cluster.url` | 种子节点地址列表，逗号分隔（`host:port`） | — |
| `port` | `jmqx.cluster.port` | 本节点集群通信端口（ScaleCube transport） | `7771` |
| `node` | `jmqx.cluster.node` | 本节点名称，**集群内唯一** | `node-1` |
| `namespace` | `jmqx.cluster.namespace` | 集群命名空间，**各节点必须一致**才能互通 | `jmqx-broker` |
| `suspicionMult` | — | 成员怀疑倍数（ScaleCube membership） | `10` |
| `pingTimeout` | — | 故障检测 Ping 超时（毫秒） | `3000` |
| `clusterMessageBufferSize` | — | 集群消息 Sink 缓冲区大小 | `1024` |
| `external.host` | — | 容器/云环境对外暴露 IP（NAT 场景） | — |
| `external.port` | — | 容器/云环境对外暴露端口 | — |

### 集群消息扩散线程池（Broker 侧，1.4.19+）

PUBLISH 扩散、订阅关系同步等走 `Schedulers.newBoundedElastic("jmqx-cluster")`，与平台回调 `jmqx-dispatch`、业务 `jmqx-publish`/`jmqx-control` 隔离。字段定义在 `MqttConfiguration`（非 `ClusterConfig`）：

| 字段（Java） | Spring 示例属性 | 说明 | 默认值 |
|---|---|---|---|
| `clusterThreadSize` | `jmqx.tcp.cluster-thread-size` | `jmqx-cluster` 线程数；`null`/`<=0` 使用内置默认 | `max(N*2, 8)` |
| `clusterQueueSize` | `jmqx.tcp.cluster-queue-size` | 扩散任务队列；`null`/`<=0` 回退 `businessQueueSize` | 回退 businessQueue |

另见平台回调池（与集群扩散同类隔离）：

| 字段（Java） | Spring 示例属性 | 说明 | 默认值 |
|---|---|---|---|
| `dispatchThreadSize` | `jmqx.tcp.dispatch-thread-size` | `jmqx-dispatch` 线程数；`null`/`<=0` 回退 `businessThreadSize` | 回退 business |
| `dispatchQueueSize` | `jmqx.tcp.dispatch-queue-size` | 平台回调队列；`null`/`<=0` 回退 `businessQueueSize` | 回退 businessQueue |

完整线程模型、Auth/ACL Offload 与 `requiresOffload()` 说明见 [jmqx-broker 线程模型](../jmqx-broker/README.md#线程模型)。

### 配置要点

- **MQTT 端口与集群端口分离**：`MqttConfiguration.port`（如 `1883`）服务设备；`cluster.port`（如 `7771`）仅用于节点间通信。
- **种子列表**：`url` 建议包含所有节点（或稳定种子），本节点地址会被自动过滤。
- **命名空间**：`namespace` 不一致的节点互不可见，可用于同进程多集群隔离（测试常见）。
- **集群 ID**：`namespace:node`（见 `ClusterConfig.getClusterId()`），用于会话/主题路由归属。
- **Broker 侧配置仍生效**：鉴权/ACL（`auth*` / `acl*`，含 `requiresOffload`）、连接上限、`business*` / `dispatch*` / `clusterThread*` 等见 [jmqx-broker 配置项](../jmqx-broker/README.md#配置项)。集群节点转发的 PUBLISH 会跳过设备侧 ACL，设备入口节点仍做 ACL。

### 最小可用示例（YAML / Spring）

```yaml
jmqx:
  tcp:
    port: 1883
    # 可选：调大集群扩散池，避免与平台回调争用
    # cluster-thread-size: 16
    # cluster-queue-size: 100000
    # dispatch-thread-size: 16
  cluster:
    enable: true
    namespace: jmqx
    node: node-1
    port: 7771
    url: 127.0.0.1:7771,127.0.0.1:7772
```

## 关键实现

- `ScubeClusterRegistry`：基于 ScaleCube 的 `ClusterRegistry` SPI 实现
- `JacksonMessageCodec`：使用 Jackson 进行集群消息的序列化/反序列化
- 前缀索引 + LRU 匹配缓存：主题路由使用前缀分桶加速（`extractPrefix`），匹配结果使用 512 条目 LRU 缓存
- 故障自动清理：节点离开集群时，自动清理该节点的会话路由和主题路由
