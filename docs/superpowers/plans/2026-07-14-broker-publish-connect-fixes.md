# 计划：定向投递订阅校验与同 clientId 接管修复

- **日期**: 2026-07-14
- **对应设计**: [2026-07-14-broker-publish-connect-fixes.md](../specs/2026-07-14-broker-publish-connect-fixes.md)
- **状态**: 已实施（待用户 review 后提交）

## 任务清单

### Task 1 — PublishProcessor 定向投递订阅校验

- [x] `send(clientId, ...)` 通过 `TopicRegistry.getSubscribesByTopic` 过滤目标 `clientId`
- [x] 无匹配订阅则 skip + debug
- [x] 有匹配则使用订阅 QoS 写入
- [x] 更新 `MessageDispatcher` / `MqttMessageDispatcher` 文档说明

### Task 2 — SessionRegistry 实例安全关闭

- [x] `DefaultSessionRegistry.close` 改为 `sessions.remove(clientId, session)`
- [x] `SessionRegistry` 接口补充契约说明

### Task 3 — ConnectProcessor 接管与 MQTT 5 DISCONNECT

- [x] 新增 `takeOverSession`：清 Will → MQTT5 发 `0x8E` → `close`
- [x] `MqttMessageBuilder.disconnectMessage(byte reasonCode)`
- [x] `close()` 中集群 `unregisterSession` 前校验当前映射会话实例
- [x] 更新注释，移除「待实现 / 存在 BUG」标记

### Task 4 — 测试

- [x] `testPublishToClient`：先 `subscribe` 再断言收到
- [x] 新增 `testPublishToClientWithoutSubscribe`：未订阅不应收到
- [ ]（可选后续）同 clientId 轮番重连下 `SessionRegistry` 不丢失新会话的专用单测

### Task 5 — 文档

- [x] 本计划与设计规格（日期 2026-07-14）
- [x] 根 `README.md` / `jmqx-broker/README.md` 改进点与定向下发说明同步

## 验证建议（review 前）

```bash
# 单节点集成测试（需开启开关）
mvn -pl jmqx-broker -Djmqx.integration.tests=true -Dtest=BootstrapTest#testPublishToClient,BootstrapTest#testPublishToClientWithoutSubscribe test

# MQTT5 同 clientId 接管（client 模块 IT，EmbeddedBroker 使用 KICK）
mvn -pl jmqx-client -Dtest=Mqtt5ClientIT#duplicateClientIdKicksOldConnection test
```

## 合入说明

- **不在本轮自动 git commit**；待人工 review 确认后再提交。
