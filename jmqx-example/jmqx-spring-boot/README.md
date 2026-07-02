# jmqx-spring-boot — Spring Boot 集成示例

展示如何将 jmqx-broker 嵌入到 Spring Boot 应用中，实现 MQTT 设备接入平台。

## 功能演示

- **Spring Boot 自动配置**：使用 `@Component` + `@Value` 注入 MQTT 配置
- **设备鉴权**：`PlatformAuthManager` 实现固定用户名密码校验
- **主题访问控制**：`PlatformAclManager` 演示 ACL SPI 接入（默认全部放行）
- **设备生命周期**：`PlatformMessageDispatcher` 监听连接、断开、消息上报事件
- **SSL/TLS**：支持通过 `application.yml` 配置 SSL 证书（classpath 或绝对路径）
- **集群**：支持通过配置快速开启集群模式

## 项目结构

```
src/main/java/plus/jmqx/example/
├── Application.java                          # Spring Boot 入口
└── broker/
    ├── PlatformMqttBroker.java               # Broker 启动器
    ├── acl/
    │   └── PlatformAclManager.java           # 主题访问控制实现
    ├── auth/
    │   └── PlatformAuthManager.java          # 设备鉴权实现
    ├── config/
    │   └── MqttBrokerConfigurationFactory.java  # MQTT 配置工厂
    └── dispatch/
        ├── PlatformMessageDispatcher.java    # 消息分发实现
        ├── ConnectionProcessor.java          # 连接事件处理
        ├── DisconnectionProcessor.java       # 断开事件处理
        └── PublishMessageProcessor.java      # 消息上报处理
```

## 快速启动

```shell
mvn spring-boot:run -pl jmqx-example/jmqx-spring-boot
```

启动后控制台输出：

```
14:13:53.734 [jmqx-event-loop-*] INFO ... - mqtt broker start success port 1883
14:13:53.901 [jmqx-event-loop-*] INFO ... - mqtts broker start success port 8883
14:13:53.902 [jmqx-event-loop-*] INFO ... - mqtt-ws broker start success port 1884
14:13:53.906 [jmqx-event-loop-*] INFO ... - mqtt-wss broker start success port 8884
```

## 配置说明

`application.yml` 包含完整的配置模板，支持以下配置块：

| 配置前缀 | 说明 |
|---|---|
| `jmqx.tcp.*` | MQTT 传输层配置（端口、线程、缓冲区等） |
| `jmqx.ssl.*` | SSL/TLS 配置（证书路径、启用开关） |
| `jmqx.auth.fixed.*` | 固定鉴权的用户名密码 |
| `jmqx.cluster.*` | 集群配置（启用、种子节点等） |

## 技术栈

- Spring Boot 2.7.18
- jmqx-broker（内嵌）
- Reactor Netty
- Lombok
- Hutool

## 网络依赖

由于 jmqx-broker 使用 `reactor-netty`，项目中必须显式声明 `reactor-bom` 与 Spring Boot 的 BOM 一起管理，
以解决依赖冲突。详见 `pom.xml` 中的 `<dependencyManagement>` 配置。