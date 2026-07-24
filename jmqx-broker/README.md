# jmqx-broker — 核心 MQTT Broker 库

Jmqx 的核心模块，提供完整的 MQTT Broker 实现。作为一个可内嵌的库，为应用提供 MQTT 设备接入能力。

## 功能特性

- **多协议支持**：同时支持 MQTT、MQTTS（TLS）、MQTT-WebSocket、MQTT-WSS 四种传输层
- **MQTT 协议版本**：支持 v3.1、v3.1.1、v5（v5 部分实现）
- **可插拔鉴权**：通过 `AuthManager` SPI 自定义设备连接鉴权
- **主题访问控制**：通过 `AclManager` SPI 自定义发布/订阅权限
- **设备生命周期钩子**：通过 `PlatformDispatcher` 监听设备上线、下线、消息上报
- **命名空间隔离**：同一进程内可通过不同命名空间启动多个独立 Broker 实例
- **速率限制**：内置令牌桶实现连接速率控制
- **海量设备支撑**：可配置最大连接数、离线消息上限、飞行窗口上限、订阅上限等
- **指标 SPI**：`MetricsManager` 可替换的监控指标实现
- **消息拦截器链**：支持自定义拦截器处理消息分发管线
- **定向下发**：支持通过 clientId 向指定设备下发消息；**目标须已订阅该主题**（未订阅则不下发，符合 MQTT 订阅语义），走完整分发管线（含 ACL）

## 架构概览

```
┌─────────────────────────────────────────────┐
│                 Bootstrap                   │
│  (服务入口，组装 Transport + 依赖注入)         │
└──────────────┬──────────────────────────────┘
               │
    ┌──────────┼──────────┐
    ▼          ▼          ▼
┌────────┐┌────────┐┌────────┐
│ MQTT   ││ MQTTS  ││ MQTT-  │
│ TCP    ││ TLS    ││ WS/WSS │
└───┬────┘└───┬────┘└───┬────┘
    │         │         │
    └─────────┼─────────┘
              ▼
┌─────────────────────────┐
│    MessageDispatcher    │
│  (消息分发 + 拦截器链)    │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  MessageProcessor 集合   │
│  (Connect / Publish /   │
│   Subscribe / Unsub     │
│   PubAck / ...)          │
└─────────────────────────┘
```

## 核心组件

| 组件 | 说明 |
|---|---|
| `Bootstrap` | 服务入口，接收 `MqttConfiguration` 及用户 SPI 实现，启动四种传输层 |
| `Transport` | 传输层抽象，实现 TCP/TLS/WS/WSS 四种 Netty 服务 |
| `MessageDispatcher` | 消息分发器，根据 MQTT 消息类型路由到对应 Processor |
| `MessageProcessor` | 消息处理器，处理 Connect/Publish/Subscribe/Unsubscribe 等 |
| `SessionRegistry` | 会话注册中心，管理 clientId 与连接的映射 |
| `TopicRegistry` | 主题注册中心，管理主题订阅关系 |
| `MessageRegistry` | 消息注册中心，管理 Retain 消息 |
| `AclManager` | 主题访问控制 SPI |
| `AclExecutor` | ACL 卸载执行器（独立线程池，避免阻塞 jmqx-publish/control） |
| `AuthManager` | 设备连接鉴权 SPI |
| `AuthExecutor` | 鉴权卸载执行器（独立线程池，与 ACL 池隔离） |
| `PlatformDispatcher` | 设备生命周期事件回调 SPI |
| `MetricsManager` | 指标收集 SPI |
| `Interceptor` | 消息分发拦截器链 SPI |

## 快速开始

```xml
<dependency>
    <groupId>plus.jmqx.iot</groupId>
    <artifactId>jmqx-broker</artifactId>
    <version>1.4.18</version>
</dependency>
```

```java
// @formatter:off
MqttConfiguration config = new MqttConfiguration();
Bootstrap bootstrap = new Bootstrap(config);
bootstrap.startAwait();
// @formatter:on
```

详细用法参见项目根目录 README.md。

## 配置项

通过 `MqttConfiguration`（Java）或 Spring 示例中的 `jmqx.*` 属性注入。下表默认值以 `MqttConfiguration` 字段默认值为准；`N` 表示 `Runtime.availableProcessors()`。

### 端口与传输

| 字段（Java） | Spring 示例属性 | 说明 | 默认值 |
|---|---|---|---|
| `port` | `jmqx.tcp.port` | MQTT TCP 端口 | `1883` |
| `securePort` | `jmqx.tcp.secure-port` | MQTTS 端口 | `8883` |
| `websocketPort` | `jmqx.tcp.websocket-port` | MQTT over WebSocket 端口 | `1884` |
| `websocketSecurePort` | `jmqx.tcp.websocket-secure-port` | MQTT over WSS 端口 | `8884` |
| `websocketPath` | `jmqx.tcp.websocket-path` | WebSocket 路径 | `/mqtt` |
| `wiretap` | `jmqx.tcp.wiretap` | Netty 二进制日志（需 DEBUG） | `true` |
| `messageMaxSize` | `jmqx.tcp.message-max-size` | 单帧最大字节数 | `4194304`（4MB） |
| `options` | `jmqx.tcp.options` | Netty ServerBootstrap Option | — |
| `childOptions` | `jmqx.tcp.child-options` | Netty Child Option | — |

### 线程模型

```
EL (jmqx-event-loop) ──emit──► jmqx-publish / jmqx-control (parallel, 非阻塞)
                                    │
                    Auth/Acl Offload (jmqx-auth-io / jmqx-acl-io)
                                    │ 完成后回流 publish/control
                                    ▼
              Platform ──► jmqx-dispatch (boundedElastic)
              Cluster  ──► jmqx-cluster  (boundedElastic, 与平台隔离)
```

| 字段（Java） | Spring 示例属性 | 说明 | 默认值 |
|---|---|---|---|
| `bossThreadSize` | `jmqx.tcp.boss-thread-size` | Netty Boss 线程数 | `N` |
| `workThreadSize` | `jmqx.tcp.work-thread-size` | Netty Worker 线程数 | `max(N*2, 8)` |
| `businessThreadSize` | `jmqx.tcp.business-thread-size` | 业务 parallel 总数（约 3:1 拆为 `jmqx-publish` / `jmqx-control`） | `max(N*2, 8)` |
| `businessQueueSize` | `jmqx.tcp.business-queue-size` | 业务分发 Sink 队列容量 | `100000` |
| `dispatchThreadSize` | `jmqx.tcp.dispatch-thread-size` | 平台回调 `jmqx-dispatch` 线程数；`<=0` 回退 business | 回退 business |
| `dispatchQueueSize` | `jmqx.tcp.dispatch-queue-size` | 平台回调队列；`<=0` 回退 businessQueue | 回退 businessQueue |
| `clusterThreadSize` | `jmqx.tcp.cluster-thread-size` | 集群扩散 `jmqx-cluster` 线程数 | `max(N*2, 8)` |
| `clusterQueueSize` | `jmqx.tcp.cluster-queue-size` | 集群扩散队列；`<=0` 回退 businessQueue | 回退 businessQueue |

> `jmqx-publish` / `jmqx-control` 为 Reactor `newParallel`（假定非阻塞）。Auth/ACL **不占用**该池；完成后会回流对应 Scheduler 再做匹配、会话与写回编排。`Interceptor` 必须非阻塞，见接口契约。

### 鉴权 / ACL 卸载线程池

用户自定义 `AuthManager` / `AclManager` 可能包含 Feign、DB 等阻塞调用。Broker 会将其切到独立线程池，避免在 `jmqx-publish` / `jmqx-control`（Reactor NonBlocking）上触发 `block()` 异常或拖死心跳。

**Auth 与 ACL 默认使用两套独立线程池**（`jmqx-auth-io-*` / `jmqx-acl-io-*`），避免 PUBLISH 风暴饿死 CONNECT 鉴权。保持专用 `OffloadExecutor`，不使用全局 `Schedulers.boundedElastic()`。

| 字段（Java） | Spring 示例属性 | 说明 | 默认值 |
|---|---|---|---|
| `authTimeoutMillis` | `jmqx.tcp.auth-timeout-millis` | 鉴权超时（毫秒），超时视为失败 | `1000` |
| `authThreadSize` | `jmqx.tcp.auth-thread-size` | 鉴权线程池大小 | `max(N*4, 16)` |
| `authQueueSize` | `jmqx.tcp.auth-queue-size` | 鉴权队列容量，满则拒绝连接 | `200000` |
| `aclTimeoutMillis` | `jmqx.tcp.acl-timeout-millis` | ACL 超时（毫秒），超时视为拒绝 | `1000` |
| `aclThreadSize` | `jmqx.tcp.acl-thread-size` | ACL 线程池大小 | `max(N*4, 16)` |
| `aclQueueSize` | `jmqx.tcp.acl-queue-size` | ACL 队列容量，满则拒绝发布/订阅 | `200000` |

> Spring 示例里 `auth-thread-size` / `acl-thread-size` 等为 `0` 时表示不覆盖，沿用 `MqttConfiguration` 默认值。

### 水位与读写限流

| 字段（Java） | Spring 示例属性 | 说明 | 默认值 |
|---|---|---|---|
| `lowWaterMark` | `jmqx.tcp.low-water-mark` | Netty 写缓冲低水位（字节） | `65536`（64KB） |
| `highWaterMark` | `jmqx.tcp.high-water-mark` | Netty 写缓冲高水位（字节） | `1048576`（1MB） |
| `globalReadWriteSize` | `jmqx.tcp.global-read-write-size` | 全局读写限速，`读,写`（字节/秒） | `10000000,100000000` |
| `channelReadWriteSize` | `jmqx.tcp.channel-read-write-size` | 单连接读写限速，`读,写`（字节/秒） | `10000000,100000000` |

### SSL

| 字段（Java） | Spring 示例属性 | 说明 | 默认值 |
|---|---|---|---|
| `sslEnable` | `jmqx.ssl.enable` | 是否启用 SSL | `false` |
| `sslCa` | `jmqx.ssl.ca` | CA 证书路径 | — |
| `sslCrt` | `jmqx.ssl.crt` | 服务端证书路径 | — |
| `sslKey` | `jmqx.ssl.key` | 服务端私钥路径 | — |

> Spring 示例额外支持 `jmqx.ssl.mode`（`classpath` / `absolute-path`）解析证书路径。

### 连接行为与容量

| 字段（Java） | 说明 | 默认值 |
|---|---|---|
| `connectMode` | 重复 `clientId`：`UNIQUE` 拒绝新连接 / `KICK` 踢掉旧连接 | `UNIQUE` |
| `notKickSeconds` | `KICK` 模式下，连接建立后若干秒内不踢出 | `30` |
| `maxConnections` | 最大连接数，`0`=不限制 | `0` |
| `connectionRateLimit` | 连接速率上限（连接/秒），`0`=不限制 | `0` |
| `maxOfflineQueueSize` | 每客户端离线消息队列上限，`0`=不限制 | `0` |
| `maxTotalOfflineMessages` | 总离线消息上限，`0`=不限制 | `0` |
| `maxRetainMessageCount` | Retain 消息总数上限，`0`=不限制 | `0` |
| `maxInflightQos2` | 每会话 QoS2 飞行窗口，`0`=不限制 | `0` |
| `maxTopicSubscriptions` | 每会话订阅数上限，`0`=不限制 | `0` |
| `metricsEnabled` | 是否启用指标收集 | `false` |

### 集群

集群相关字段见 [`jmqx-cluster/README.md`](../jmqx-cluster/README.md#集群配置)，对应 `MqttConfiguration.ClusterConfig`。

## 压力测试

单节点（M1 Mac mini）200 线程持续 600 秒，64 字节负载，稳定达到约 **122,000 msg/s** 吞吐。
集群双节点约 **95,000 msg/s**。详细压测配置与方法见根目录 README。
