# jmqx-client — MQTT 客户端库（开发中）

Jmqx 的 MQTT 客户端模块，提供与 jmqx-broker 通信的客户端能力。

> **当前状态**：项目启动阶段，客户端实现正在规划中，尚无可用的源代码。

## 计划特性

- 基于 Reactor Netty 的异步 MQTT 客户端
- 支持 MQTT v3.1.1 / v5
- 自动重连与会话恢复
- 完整的 QoS 0/1/2 支持
- 响应式 API（Project Reactor）
- SSL/TLS 支持
- WebSocket 支持
- 轻量级，无额外运行时依赖

## 依赖

待客户端实现后，引入方式：

```xml
<dependency>
    <groupId>plus.jmqx.iot</groupId>
    <artifactId>jmqx-client</artifactId>
    <version>1.4.14-SNAPSHOT</version>
</dependency>
```

## 技术栈

- Reactor Netty
- Netty MQTT Codec
- Project Reactor
- JDK 17+