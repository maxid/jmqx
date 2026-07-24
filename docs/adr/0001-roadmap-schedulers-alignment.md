# Roadmap：ADR-0001 Schedulers 选型 vs jmqx 实现对标与整改

- **日期**: 2026-07-24
- **对照**: [ADR-0001](./0001-reactor-schedulers-for-mqtt-broker.md)
- **范围**: `jmqx-broker` + `jmqx-cluster`（生产路径）
- **结论**: **需要整改**，但属「加固与补齐」而非推倒重来；Auth/ACL 隔离与 publish/control 分流已对齐 ADR 主干，缺口集中在 **ACL 回调后的热路径归属、集群执行域隔离、平台/集群共享 boundedElastic、可观测性**。

---

## 1. 总判

| 维度 | 判断 |
|------|------|
| 是否需要整改 | **是** |
| 紧急程度 | **P0 有 1 项**（ACL/Auth 回调占用阻塞池做匹配与 fan-out）；其余为 P1/P2 |
| 与 ADR 整体契合度 | **约 6.5 / 10** — 阻塞卸载方向正确；数据面回流与集群拓扑未完成 |
| 建议策略 | 分 4 期：止血 → 隔离 → 可观测 → 可选优化；不改协议语义、不强制上 persist 池（当前为内存存储） |

已对齐、**本期不必动**的部分：

- 客户端 `LoopResources`（`jmqx-event-loop`）+ `runOn`
- PINGREQ 旁路留在 EL
- `AuthExecutor` / `AclExecutor` 独立有界池 + Abort + 超时（1.4.18）
- PUBLISH / 控制消息双 Sink + `jmqx-publish` / `jmqx-control`（`newParallel`；原名 `*-io` 已去掉）
- `PlatformDispatcher` 走 `jmqx-dispatch`（`newBoundedElastic`，配置与 business 解耦）
- **已落地**：Auth/ACL 回流数据面（R1）、`jmqx-cluster` 专用池（R2）、Interceptor 非阻塞契约（R5）

---

## 2. 对标矩阵

图例：✅ 对齐 · ⚠️ 部分对齐 / 有风险 · ❌ 缺口 · — 当前产品范围外  
「判定」= 整改前快照；「整改后判定」= R1/R2/R4/R5 落地后（见 §7）。

| ADR 业务 / 决策 | 推荐执行域 | jmqx 现状（整改前） | 判定 | 整改后判定 | 证据（入口） |
|-----------------|------------|-------------------|------|------------|--------------|
| TCP Codec / Accept | EL | `jmqx-event-loop` | ✅ | ✅ | `AbstractReceiveContext` / `Mqtt*Receiver` |
| Keepalive / PING | EL | EL 旁路写 PONG | ✅ | ✅ | `MqttReceiveContext` |
| CONNECT 鉴权 | auth 专用池 | `jmqx-auth-io`（`OffloadExecutor`） | ✅ | ✅ | `AuthExecutor` / `ConnectProcessor` |
| SUB/PUB ACL | acl 专用池 | `jmqx-acl-io` | ✅ | ✅ | `AclExecutor` / `PublishProcessor` / `SubscribeProcessor` |
| 鉴权/ACL **之后** 匹配、会话、fan-out | EL / parallel（数据面） | 多在 **auth/acl 回调线程** 继续 | ⚠️→❌ | ✅ | R1：`scheduleOnPublish` / `scheduleOnControl` 回流 |
| 主题匹配 + 大扇出 | EL 或 `parallel` | 常在 **acl 池**；入口曾在 publish-io | ⚠️ | ✅ | 回流后在 `jmqx-publish` / `jmqx-control` |
| QoS 内存状态机 | 宜连接 EL 亲和 | 在 publish-io / acl / control-io 上改 session | ⚠️ | ⚠️ | 已离开 acl 池；EL 亲和仍待 R7 |
| 写回客户端 | 目标连接 EL | `outbound().sendObject`（一般安全） | ✅/⚠️ | ✅/⚠️ | 写安全；可变状态亲和未文档化（R7） |
| 平台 SPI 回调 | blocking 池 | `jmqx-dispatch-io`（与 business 同尺寸） | ✅ | ✅ | `jmqx-dispatch`；R4 配置已拆分 |
| 用户 `Interceptor` | 默认按阻塞 | **同步**跑在 dispatch 调用线程 | ⚠️ | ⚠️ | R5：非阻塞契约已文档化；仍同步执行 |
| 集群 PUBLISH 扩散 | cluster-el / 专用 | 全局 `Schedulers.boundedElastic()` | ❌ | ✅ | R2：`jmqx-cluster`（`MessageProxy`） |
| 集群订阅同步 | 同上 | 同上 | ❌ | ✅ | R2：`Subscribe`/`Unsubscribe`/`Connect` 走 `SchedulerTasks.subscribeOnCluster` |
| 独立 `cluster-el` | 与 client-el 分离 | ScaleCube 自带 transport，**未**与 client LoopResources 隔离 | ❌ | ❌ | `ScubeClusterRegistry`（R3 未做） |
| `cluster-ctrl`（single） | 成员/路由串行 | 无；`ConcurrentHashMap` 多线程更新 | ❌ | ❌ | 集群路由相关（R3 未做） |
| persist 池 | 磁盘/DB 持久化 | 默认内存 `MessageRegistry` | — | — | 上磁盘 SPI 后再建（R9） |
| bridge 池 | Webhook 等 | 无内置 bridge | — | — | |
| 池监控 / 队列深度 | ADR §8 | 缺统一 metrics | ❌ | ❌ | R8 未做 |
| D5 禁止 EL/parallel 阻塞 | — | Auth/ACL 已卸载；Interceptor 仍可能堵 | ⚠️ | ⚠️ | Offload+回流已加固；Interceptor 仍依赖契约 |

### 当前线程流（简图）

```
EL (jmqx-event-loop)
  └─ emit → publishOn → jmqx-publish / jmqx-control
       └─ Auth/Acl Offload (jmqx-auth-io / jmqx-acl-io)
            └─ scheduleOnPublish/Control 回流 ✅
                 ├─ Platform → jmqx-dispatch ✅
                 └─ Cluster spread → jmqx-cluster ✅
```

---

## 3. 是否整改：按优先级

### P0 — 应做（正确性 / 隔离失效）

| ID | 问题 | 影响 | 整改方向 | 验收 |
|----|------|------|----------|------|
| **R1** | ACL/Auth 通过后，主题匹配、retain、QoS ACK、fan-out、会话注册仍在 **offload 池** 线程执行 | ACL 风暴同时打满匹配与写状态；auth 池被会话逻辑占用；违背「阻塞池只做阻塞」 | ACL/Auth `whenComplete`/`thenAccept` 内只做通过/拒绝判定，**`publishOn(jmqx-publish-io|control-io)` 或显式投回原 Sink/调度器** 再跑 `processAuthorized` / 会话后续 | 慢 ACL 注入时：publish-io CPU 仍可匹配；acl 池线程 dump 无大扇出栈；PUBLISH P99 不随 ACL RT 同比例恶化 |

### P1 — 建议做（ADR 拓扑补齐）

| ID | 问题 | 影响 | 整改方向 | 验收 |
|----|------|------|----------|------|
| **R2** | 集群扩散使用全局 `Schedulers.boundedElastic()` | 与其他阻塞任务争用；不可命名监控；不符合 cluster 专用池 | 引入 `jmqx-cluster-io`（`newBoundedElastic` 或 `fromExecutor`），替换 `MessageProxy` / Sub/Unsub/Connect 四处 `subscribeOn` | 线程名可见；压测下平台回调打满不影响集群 spread 队列 |
| **R3** | 无独立集群控制面 / 与 client-el 隔离不足 | 成员抖动、路由更新与客户端 EL 互相影响（取决于 ScaleCube 实现） | 评估 ScaleCube：能注入 transport 线程则建 `cluster-el`；路由表合并考虑 `newSingle("jmqx-cluster-ctrl")` 或明确文档「CHM 无锁合并即可」 | 集群心跳超时次数在客户端大流量下不抬升；或文档记录「接受 CHM、不做 single」的 ADR 偏差决议 |
| **R4** | `businessThreadSize` 同时驱动 publish/control 与 `jmqx-dispatch-io` | 调协议池误伤平台回调容量 | 拆配置：`dispatchThreadSize` / `dispatchQueueSize`（可默认等于 business） | 配置项分离；README 更新 |
| **R5** | 用户 `Interceptor` 默认同步、无 offload 契约 | 阻塞插件可污染 emit / 业务线程 | 文档强制「Interceptor 必须非阻塞」；或提供 `OffloadInterceptor` 包装 / SPI 标注 | README + Javadoc；可选单测证明慢 interceptor 不堵 EL |

### P2 — 可选优化（性能 / 演进）

| ID | 问题 | 影响 | 整改方向 | 验收 |
|----|------|------|----------|------|
| **R6** | 全部 PUBLISH 离开 EL 进 parallel | 多一次切换；小消息场景可能不如留 EL | 基准：小包 / 大扇出对比「EL 直处理」vs「publish-io」；再决定是否对 QoS0 短路径旁路 | 基准报告入库；有数据再改默认 |
| **R7** | Session 可变状态无 EL 亲和说明 | 隐蔽并发风险（ADR 反模式 #5） | 梳理 `MqttSession` 字段并发；能锁/原子则文档化；关键写可 `eventLoop().execute` | 竞态测试或审查记录 |
| **R8** | 缺 Scheduler / Offload 池指标 | 无法按 ADR §8 验收 | 暴露队列深度、活跃线程、拒绝次数、超时次数到 `MetricsManager` | 指标可 scrape；故障注入可见 |
| **R9** | 若引入磁盘/DB 会话或 retain | 会与 auth/acl 争用若仍无 persist 池 | SPI 落盘时同步加 `jmqx-persist-io` | 与 auth/acl 隔离压测 |
| **R10** | Auth/Acl 为裸 `ThreadPoolExecutor`，未 `Schedulers.fromExecutor` | 与 Reactor 生态组合略别扭 | 保持现状可接受；若统一 `publishOn` 回流可再包一层 Scheduler | 以 R1 设计为准，不单独为包一层而改 |

### 明确不做（本 roadmap 范围外）

- 为「对齐 ADR 名词」强行引入全局 `Schedulers.parallel()`（已有命名 parallel 池）
- 在无磁盘持久化前预建空转 `persist` 池
- 重写 ScaleCube 或换集群库
- 要求用户 Auth/Acl 全部改成异步 API（保留同步 SPI + Offload 是合理选择）

---

## 4. 分期计划

### 一期（止血）— 目标 1～2 PR

1. **R1**：ACL/Auth 完成后回流 `jmqx-publish-io` / `jmqx-control-io`
2. 补充故障注入测试：慢 ACL（`Thread.sleep` / 延迟）下 fan-out 不在 `jmqx-acl-io-*` 线程
3. 更新 `jmqx-broker/README.md` 线程模型：画清「offload → 回流数据面」

**退出标准**：R1 验收通过；无协议行为变更（CONNACK/PUBACK 语义不变）。

### 二期（隔离）— 目标 1～2 PR

1. **R2**：`jmqx-cluster-io` 专用池 + 替换全局 `boundedElastic`
2. **R4**：拆分 dispatch 线程配置
3. **R5**：Interceptor 非阻塞契约（文档优先；包装可选）

**退出标准**：集群 spread 与平台回调线程名分离；配置项文档化。

### 三期（可见性 + 集群评估）— 目标调研 + 可选 PR

1. **R8**：Offload / Dispatcher / Cluster 池指标
2. **R3**：ScaleCube 线程模型调研结论写入短 ADR 附录或本文件更新（做 / 不做 cluster-el、cluster-ctrl）
3. **R7**：Session 并发审查纪要

**退出标准**：有监控看板或 Metrics SPI 字段；集群项有明确「做或不做」决议。

### 四期（按需）

1. **R6** 基准驱动的 PUBLISH 旁路
2. **R9** 持久化 SPI 出现时再开 persist 池

---

## 5. 建议落地顺序（依赖）

```
R1（回流数据面）
  ├─→ R8（指标，验证 R1）
  ├─→ R2（集群池，同类 subscribeOn 清理）
  ├─→ R4（配置拆分，改 Bootstrap 时顺手）
  └─→ R5（契约）
R3 / R7（调研，可与二期并行）
R6 / R9（有数据或有需求再开）
```

---

## 6. 风险与兼容性

| 风险 | 缓解 |
|------|------|
| R1 回流增加一次调度，可能抬升 PUBLISH 延迟 | 用压测对比；延迟换隔离通常可接受；可保留「ACL 同步快速路径」配置（默认 off） |
| R2 改 Scheduler 后集群扩散背压行为变化 | 保持有界队列 + 明确溢出策略（现有 Mono subscribe 需确认错误处理） |
| 回流后 ByteBuf retain/release 时序变化 | R1 必须带 retain 生命周期单测（现有 `buf.retain` 在 ACL 路径已存在） |
| 行为「看起来变慢」被误报为回归 | 对比指标用 P99 + acl 池占用，而非仅平均 RT |

---

## 7. 对标勾选（滚动更新）

- [x] 盘点 `subscribeOn` / `publishOn` / `LoopResources` / Offload
- [x] R1 回流落地（Auth/ACL → `jmqx-publish` / `jmqx-control`）
- [x] R2 集群专用池（`jmqx-cluster`）
- [x] R4 dispatch 配置拆分（`dispatchThreadSize` / `dispatchQueueSize`）
- [x] R5 Interceptor 契约（Javadoc + README）
- [ ] R8 池指标
- [ ] R3 集群 EL/ctrl 决议
- [ ] R7 Session 亲和审查
- [ ] R6 基准报告
- [ ] 偏差项回写 ADR 附录或独立「jmqx 线程模型 ADR」（绑定本仓库）

---

## 8. 一句话给决策者

**一期/二期整改已落地（R1/R2/R4/R5）**：Offload 池与匹配/fan-out 分离，集群与平台回调分池，Scheduler 命名去掉误导性 `-io`。后续按 R8/R3/R7 补可观测与集群评估即可。