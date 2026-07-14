# jmqx-broker：定向投递订阅校验与同 clientId 接管修复

- **日期**: 2026-07-14
- **状态**: 已定稿（开发完成，待 review 合入）
- **范围**: `jmqx-broker`（`PublishProcessor` / `ConnectProcessor` / `DefaultSessionRegistry`）
- **关联**: MQTT 订阅语义、OASIS MQTT v3.1.1 / v5.0 §3.1.4 Session taken over

## 1. 背景

### 1.1 定向投递绕过订阅

平台可通过 `MessageDispatcher.publish(clientId, message)` 向指定设备下发消息。README（1.4.12）约定「在设备订阅同一个主题情况下」下发，但 `PublishProcessor` 定向分支仅按 `clientId` 查 Session 并写入，**未校验订阅关系**，导致未订阅主题的客户端仍能收到 PUBLISH，不符合 MQTT 规范。

### 1.2 同 clientId 多连接竞态与 MQTT 5 Reason Code

`ConnectMode.KICK` 下保护期外踢旧连接时：

1. 直接 `oldSession.close()` → `connection.dispose()` → `registryClose` 异步/延后回调；
2. `DefaultSessionRegistry.close` 仅按 `clientId` `remove`，不校验实例；
3. 新会话注册后，旧连接 dispose 回调再 `remove(clientId)` / `unregisterSession(clientId)`，**误删新会话及集群路由**。

同时注释标明 MQTT 5.0 应下发 `DISCONNECT` Reason Code `0x8E`（Session taken over），此前未实现。

## 2. 设计决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 定向投递订阅校验 | 走 `TopicRegistry.getSubscribesByTopic` + `clientId` 过滤 | 复用现有通配匹配与 QoS 降级，与广播路径一致 |
| 未订阅时行为 | 跳过下发 + debug 日志 | 符合 MQTT；平台侧可依赖「未订阅=不到达」语义 |
| Session 移除 | `ConcurrentHashMap.remove(clientId, session)` 实例校验 | 修复轮番重连丢会话 |
| 集群 unregister | 仅当 registry 当前映射仍为本会话（或已为空）时摘路由 | 与 SessionRegistry 对称，避免误摘新连接节点路由 |
| MQTT 5 踢连接 | `writeAndFlush(DISCONNECT 0x8E)` 后再关 TCP；清 Will | 符合 §3.1.4；避免异步写被 dispose 冲掉；避免接管误发 Will |
| MQTT 3.x 踢连接 | 直接关 TCP | 协议无 Reason Code DISCONNECT |

## 3. 行为说明

### 3.1 定向投递（PublishProcessor）

```
publish(clientId, PUBLISH)
  → ACL（既有）
  → Session 在线？
  → TopicRegistry 匹配订阅且 session.clientId == 目标？
       否 → skip
       是 → 按订阅 QoS wrap + write
```

仍不经主题扇出（只投目标设备），但**必须满足订阅**。

### 3.2 同 clientId 接管（ConnectProcessor）

```
CONNECT 同 clientId 且旧会话 ONLINE
  UNIQUE → 拒绝新连接
  KICK + 保护期内 → 拒绝新连接
  KICK + 保护期外 → takeOverSession(old):
       setWill(null)
       MQTT5 → DISCONNECT 0x8E
       close TCP
  → 新连接继续鉴权/注册
```

`SessionRegistry.close` / 集群 `unregisterSession` 均按实例保护，旧 dispose 不再误伤新会话。

## 4. 变更文件（实现侧）

- `PublishProcessor.java` — 定向投递订阅门禁
- `ConnectProcessor.java` — `takeOverSession`、集群 unregister 实例保护
- `DefaultSessionRegistry.java` — `remove(clientId, session)`
- `MqttMessageBuilder.java` — `disconnectMessage(reasonCode)`
- `MessageDispatcher` / `MqttMessageDispatcher` — Javadoc
- `SessionRegistry` — `close` 契约说明
- 测试：`BootstrapTest.testPublishToClient`（先订阅）、新增 `testPublishToClientWithoutSubscribe`

## 5. 非目标

- 不改变 `ConnectMode.UNIQUE` 语义
- 不改变定向投递「不经主题扇出、只投目标」的产品能力
- 不在本变更中重构整条 CONNECT 鉴权异步链路
