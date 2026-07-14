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
| `AuthManager` | 设备连接鉴权 SPI |
| `PlatformDispatcher` | 设备生命周期事件回调 SPI |
| `MetricsManager` | 指标收集 SPI |
| `Interceptor` | 消息分发拦截器链 SPI |

## 快速开始

```xml
<dependency>
    <groupId>plus.jmqx.iot</groupId>
    <artifactId>jmqx-broker</artifactId>
    <version>1.4.17</version>
</dependency>
```

```java
MqttConfiguration config = new MqttConfiguration();
Bootstrap bootstrap = new Bootstrap(config);
bootstrap.startAwait();
```

详细用法参见项目根目录 README.md。

## 配置项

`MqttConfiguration` 支持丰富的配置参数，涵盖线程模型、端口、SSL、集群、限流等，详见 `MqttConfiguration.java`。

## 压力测试

单节点（M1 Mac mini）200 线程持续 600 秒，64 字节负载，稳定达到约 **122,000 msg/s** 吞吐。
集群双节点约 **95,000 msg/s**。详细压测配置与方法见根目录 README。