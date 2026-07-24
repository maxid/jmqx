# ADR-0001：基于 reactor-netty 的 MQTT Broker / Cluster 业务与 Reactor Schedulers 选型

- **状态**: Accepted（参考基线）
- **日期**: 2026-07-24
- **范围**: 通用参考；**不绑定**本仓库实现，供后续与项目实现对标
- **相关技术**: Reactor Netty、Reactor Core `Schedulers`、MQTT 3.1.1 / 5.0、Broker 集群

---

## 1. 背景与问题

用 reactor-netty 实现 MQTT Broker（含单机与集群）时，业务天然混杂三类工作：

1. **非阻塞网络 I/O**：收发包、编解码、写回、节点间转发
2. **CPU 密集短任务**：主题树匹配、QoS 状态机、会话索引、共享订阅选路
3. **阻塞 / 慢调用**：鉴权、ACL、磁盘/DB 持久化、HTTP Webhook、同步插件

Reactor 提供多类 `Scheduler`，再叠加 Netty **Event Loop**。选错的典型后果：

- 在 Event Loop 上阻塞 → 整连接组吞吐塌陷、keepalive 误判、尾延迟爆炸
- 把纯 CPU 路由丢到 `boundedElastic` → 上下文切换过多、路由延迟上升
- 所有业务挤同一弹性池 → 鉴权风暴拖死持久化 / 集群心跳

本 ADR 给出**业务 → 线程模型**的默认映射，作为架构基线与 Code Review 检查清单。

---

## 2. 决策摘要

| 决策 | 内容 |
|------|------|
| D1 | **默认留在 Netty Event Loop**：编解码、协议状态推进、内存索引读写、异步写回 |
| D2 | **CPU 密集且可能较长**：切到 `Schedulers.parallel()`（或专用 parallel 池） |
| D3 | **任何可能阻塞的调用**：切到 `Schedulers.boundedElastic()` 或**业务专用有界池** |
| D4 | **集群控制面**：独立 `single` / 小规模自定义池，与数据面隔离 |
| D5 | **禁止**在 Event Loop / `parallel` 上执行 JDBC、同步 HTTP、同步文件、`Thread.sleep`、争用严重的锁等待 |
| D6 | 优先 **Reactor 原生异步客户端**（Redis/DB/HTTP），能避免切池则不切；切池是兜底 |

---

## 3. Scheduler / 线程域一览

### 3.1 五类执行域

| 执行域 | 典型获取方式 | 线程特征 | 允许做什么 | 禁止做什么 |
|--------|----------------|----------|------------|------------|
| **EL** Event Loop | reactor-netty `LoopResources` / Channel 线程 | 固定少量 NIO 线程；同一 Channel 串行 | 非阻塞读写、轻量内存结构、`channel.write`、短临界区 CAS | 阻塞 I/O、重 CPU、长时间锁、同步日志落盘 |
| **parallel** | `Schedulers.parallel()` | 约 = CPU 核数；假定非阻塞 | 主题匹配、fan-out 计算、编解码后的纯计算、定时非阻塞任务 | 阻塞调用；无界排队业务 |
| **boundedElastic** | `Schedulers.boundedElastic()` | 弹性有界；专为阻塞设计 | JDBC、同步 HTTP、文件、LDAP、阻塞插件 | 超长占用不释放；把整个 Broker 热点路径都堆上来 |
| **single** | `Schedulers.single()` / `newSingle` | 单线程串行 | 需要全局顺序的控制面状态机、轻量定时器协调 | 重负载数据面 |
| **custom** | `Schedulers.fromExecutorService(...)` | 自管大小与隔离 | 鉴权池、持久化池、集群 Raft/Gossip、Webhook 池 | 无界无监控的裸 `CachedThreadPool` |

### 3.2 与「不切线程」的关系

reactor-netty 入站默认在 **EL** 上交付。`publishOn` / `subscribeOn` 才会换线程。

原则：

1. **能证明非阻塞且耗时极短** → 留在 EL（零切换成本）
2. **CPU 重但非阻塞** → `publishOn(parallel)`
3. **可能阻塞** → `publishOn(boundedElastic)` 或专用池；或换成异步 API 后留在 EL/parallel
4. **切池后写回客户端** → 用 Netty/Reactor 的线程安全写接口（如 `Connection.outbound()` / `channel.eventLoop().execute`），避免跨线程直接摸不安全的会话可变状态

---

## 4. MQTT 业务映射表

### 4.1 连接与会话（Session Lifecycle）

| 业务 | 工作性质 | 推荐执行域 | 说明 |
|------|----------|------------|------|
| TCP/TLS Accept、MQTT Codec | 非阻塞 I/O | **EL** | 绝不切换 |
| CONNECT 包解析与基础校验（协议版本、flags、clientId 格式） | 短 CPU | **EL** | 微秒级 |
| CONNECT 鉴权（DB / JWT 远程校验 / HTTP Auth） | 阻塞或慢 I/O | **boundedElastic** 或 **auth 专用池** | 高并发下务必隔离，避免拖垮持久化池 |
| Session 接管（踢旧连接、发 DISCONNECT 0x8E） | 协议 + 连接管理 | **EL** 协调；踢连写包在目标连接 EL | 跨连接操作用 `eventLoop.execute`，避免竞态 |
| CONNACK 写回 | 非阻塞写 | **EL** | |
| Keepalive / IdleTimeout | 定时器 | **EL**（Netty IdleState） | 不要用业务池做心跳判定 |
| DISCONNECT / 异常关闭清理 | 内存清理 + 可能持久化 | 内存：**EL**；持久化：**boundedElastic** | 拆开：先快速摘会话，再异步落盘 |
| Will 延迟发送（MQTT 5） | 延迟调度 + 再走 PUBLISH 路径 | 调度：`parallel`/`EL` schedule；投递：走统一 PUBLISH 管线 | 延迟任务本身勿阻塞 |

### 4.2 订阅面（Subscribe Path）

| 业务 | 工作性质 | 推荐执行域 | 说明 |
|------|----------|------------|------|
| SUBSCRIBE / UNSUBSCRIBE 解析 | 短 CPU | **EL** | |
| ACL 校验（内存规则） | 短 CPU | **EL** | |
| ACL 校验（远程 / DB） | 阻塞 | **boundedElastic** / **acl 池** | 可缓存命中后回 EL |
| 主题树 / 订阅索引更新 | 短～中 CPU，常需线程安全结构 | **EL** 或 **parallel** | 若用全局锁且冲突高，考虑分片索引 + parallel；避免在 EL 上长时间锁 |
| 保留消息（Retained）投递给新订阅 | 读存储 + 写连接 | 内存保留：**EL/parallel**；磁盘/DB：**boundedElastic** 读出后再 `publishOn(EL)` 写 | |
| SUBACK / UNSUBACK | 非阻塞写 | **EL** | |

### 4.3 发布面（Publish Path）— 热点路径

| 业务 | 工作性质 | 推荐执行域 | 说明 |
|------|----------|------------|------|
| PUBLISH 解码、基础校验 | 短 CPU | **EL** | |
| 发布 ACL | 同订阅 ACL | 内存 **EL**；远程 **boundedElastic** | |
| 主题匹配 + 订阅者枚举 | CPU 密集 | **优先 EL（若匹配 O(1)/很低常数）**；复杂通配 / 大扇出 → **parallel** | 这是 Broker 吞吐核心；用基准决定是否离开 EL |
| 本地 fan-out 写多个 Session | 非阻塞写 | **各目标连接的 EL** | 匹配在 A 线程，写回用目标 Channel 的 eventLoop |
| QoS 0 | 无持久化 | **EL / parallel** 计算 + EL 写 | |
| QoS 1/2 飞行窗、PUBACK/PUBREC/... 状态机 | 内存状态 | **EL**（按连接串行最省事） | 同一 client 的 QoS 状态机与连接 EL 亲和，减少锁 |
| QoS 1/2 持久化 / 会话消息落盘 | 阻塞 | **boundedElastic** 或 **persist 专用池** | 与鉴权池隔离 |
| 共享订阅（Shared Subscription）选路 | 短 CPU | **EL / parallel** | |
| 拦截器 / 插件（未知是否阻塞） | 默认按阻塞 | **boundedElastic** 或插件自带池 | 契约上要求插件非阻塞时可留 EL，但需强制规范 |

### 4.4 系统主题、桥接、管理面

| 业务 | 工作性质 | 推荐执行域 | 说明 |
|------|----------|------------|------|
| `$SYS` 指标聚合 | 定时采样 | **parallel** 或 **single** | 勿在 EL 上做重聚合 |
| 指标 scrape / JMX / Prometheus 文本 | 可能阻塞 | **boundedElastic** | |
| HTTP 管理 API | 阻塞或 Netty HTTP | 若 WebFlux 非阻塞：**EL**；Servlet/同步：**boundedElastic** | |
| 出站 Bridge（HTTP Webhook / 另一 MQTT） | 慢 I/O | 异步客户端 → **EL**；同步客户端 → **bridge 专用池** | |
| 审计日志同步写盘 | 阻塞 | **boundedElastic** / 异步 appender | 同步 `log.info` 若 appender 阻塞会污染调用线程 |

### 4.5 集群（Cluster）

把集群拆成 **数据面** 与 **控制面**：

| 业务 | 工作性质 | 推荐执行域 | 说明 |
|------|----------|------------|------|
| 节点间消息转发（TCP/QUIC/自定义 RPC） | 非阻塞网络 | **集群连接自己的 EL** | 与客户端 EL 可用不同 `LoopResources`，避免互抢 |
| 路由表查询（本节点 clientId → node） | 内存 | **EL / parallel** | |
| 路由表更新（上下线 gossip） | 短 CPU + 可能顺序 | **single** 或分片锁 + **parallel** | 控制面顺序用 single 更简单 |
| 成员变更 / Leader 选举 / Raft 日志 | 强顺序 + 可能磁盘 | **cluster-control 专用池**（常 `newSingle` 或小固定池） | **禁止**占用客户端数据面 EL |
| 跨节点转发前的本地匹配 | 同 PUBLISH | 同 4.3 | |
| 集群心跳 | 定时非阻塞 | **集群 EL** 或 control `single` | 心跳被阻塞 = 误判节点死亡 |
| 分布式会话存储（Redis 等） | 网络 I/O | **异步驱动留在 EL/parallel**；同步 Jedis → **boundedElastic** | 强烈推荐异步客户端 |
| 全量/增量订阅同步 | 中等 CPU + 网络 | 计算 **parallel**；发送 **集群 EL** | 大批量勿堵客户端 EL |

```
                    ┌─────────────────────────────────────┐
                    │         Client LoopResources         │
                    │  CONNECT/PUBLISH/SUBSCRIBE 数据面    │
                    └──────────────┬──────────────────────┘
                                   │ 仅非阻塞、短临界区
           ┌───────────────────────┼───────────────────────┐
           ▼                       ▼                       ▼
    parallel（匹配/扇出）   boundedElastic/专用池      异步客户端
           │               （鉴权/持久化/插件）         （Redis等）
           ▼                       ▼                       ▼
                    ┌─────────────────────────────────────┐
                    │        Cluster LoopResources         │
                    │     转发 / RPC / 心跳（数据面）       │
                    └──────────────┬──────────────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────────────┐
                    │   cluster-control（single/小池）      │
                    │   成员、选举、路由合并（控制面）       │
                    └─────────────────────────────────────┘
```

---

## 5. 推荐线程池拓扑（参考配置）

以下为**逻辑拓扑**，数值需按压测调整；此处给出起步量级。

| 池名 | 实现建议 | 起步规模 | 承载业务 |
|------|----------|----------|----------|
| `client-el` | Netty `LoopResources` | CPU 核数 或 2× 核数（视连接数） | 客户端连接 I/O |
| `cluster-el` | 独立 `LoopResources` | 较小固定（如 2～4） | 节点间 RPC / 转发 |
| `cpu` | `Schedulers.newParallel("mqtt-cpu", N)` | N = CPU | 重匹配、大扇出计算 |
| `blocking` | `Schedulers.boundedElastic()` 或拆分如下 | 默认有界弹性 | 兜底阻塞 |
| `auth` | `newBoundedElastic(max, queue, "mqtt-auth")` | 按鉴权 RT 与 QPS | CONNECT / 远程 ACL |
| `persist` | `newBoundedElastic(...)` 或固定 `fromExecutor` | 按磁盘/DB 并发能力 | QoS 持久化、会话存储 |
| `bridge` | 同上 | 按下游限流 | Webhook / 外部桥 |
| `cluster-ctrl` | `Schedulers.newSingle("mqtt-cluster-ctrl")` | 1（或 Raft 库自带） | 成员与元数据顺序更新 |

**隔离原则**：鉴权风暴不应占满 `persist`；集群控制延迟不应被 `bridge` 拖死。

---

## 6. 选型决策树

```
收到工作 / 回调时问：

1. 是否在 Channel / Connection 回调里且必须立刻写网络？
   └─ 是，且工作非阻塞且 < ~50µs～百µs 量级 → 留在 EL
2. 是否调用了阻塞 API，或无法证明非阻塞？
   └─ 是 → boundedElastic 或业务专用池（优先专用）
3. 是否纯 CPU、可能 > 百µs，或会拖慢同 EL 上其他连接？
   └─ 是 → parallel（或 mqtt-cpu）
4. 是否控制面、需要全局串行语义？
   └─ 是 → single / cluster-ctrl
5. 是否可用异步非阻塞客户端替代阻塞调用？
   └─ 是 → 改异步，回到 1/3，避免切池
```

经验阈值（需用压测校准，不是硬标准）：

- EL 上单次回调建议保持 **极短**；主题匹配在超大订阅表上若 P99 明显抬高，迁到 `parallel`
- `boundedElastic` 默认上限约 `10 * CPU`，生产环境应对关键路径**显式建池**并监控队列深度

---

## 7. 反模式（Code Review 红线）

1. **在 EL 上** `jdbcTemplate.query` / `httpClient.execute`（同步）/ `Files.readAllBytes` / `lock.lock()` 长等待  
2. **`subscribeOn(boundedElastic())` 包住整条 PUBLISH 热路径**（包括主题匹配与 fan-out）  
3. **全公司共用一个无界 `ExecutorService`** 跑鉴权 + 落盘 + 集群  
4. **在 `parallel` 里跑阻塞 I/O**（parallel 假定非阻塞，阻塞会饿死 CPU 工人线程）  
5. **切线程后无限制地可变共享 Session 状态**（无亲和、无同步）→ 隐蔽并发 bug  
6. **用 `Schedulers.immediate()`「假装」异步** 却仍在 EL 上做重活  
7. **集群心跳与业务阻塞池共用** → 脑裂 / 误摘节点  

---

## 8. 可观测性与验收

至少监控：

| 信号 | 含义 |
|------|------|
| Event Loop pending tasks / 任务耗时直方图 | EL 是否被堵 |
| 各 Scheduler 活跃线程、队列深度、拒绝次数 | 池是否打满 |
| CONNECT 鉴权 P99、PUBLISH 匹配 P99、落盘 P99 | 分路径是否互相拖累 |
| 集群 RTT、心跳超时次数 | 控制面是否被数据面影响 |
| 连接 keepalive 误踢率 | 经典「EL 阻塞」副作用 |

验收建议：用「慢鉴权注入」「慢磁盘注入」「大扇出 PUBLISH」三组故障注入，确认延迟隔离符合第 5 节拓扑预期。

---

## 9. 后果

### 正面

- 数据面（EL + parallel）与阻塞面、集群控制面隔离，尾延迟可控
- Review 有明确清单，减少「随手 `boundedElastic`」
- 后续可与具体项目实现逐项对标（本 ADR 有意不绑定仓库代码）

### 负面 / 成本

- 多池增加运维与调参成本
- `publishOn` 带来对象传递与可见性约束，会话状态需明确线程亲和
- 过早离开 EL 可能增加切换开销（需靠基准决定匹配是否留 EL）

### 中立

- 若全面采用异步驱动（R2DBC、lettuce、WebClient），`boundedElastic` 使用面会缩小，但**插件与遗留 JDBC** 仍需要隔离池

---

## 10. 备选方案（未采纳为默认）

| 方案 | 未作默认的原因 |
|------|----------------|
| 一切业务 `boundedElastic` | 简单但热路径延迟差、切换多 |
| 一切留在 EL | 实现省事，阻塞一点即全局事故 |
| 仅用 `parallel` 承载所有非 I/O | 阻塞会损坏 parallel 语义；控制面缺串行模型 |
| Actor 单线程邮箱（每 Session 一邮箱） | 可作进阶模型，但与 reactor-netty 默认模型叠床架屋；本 ADR 先定 Scheduler 基线 |

---

## 11. 后续对标用法（给本仓库）

对标时建议按业务行勾选：

- [ ] 实际 `subscribeOn` / `publishOn` / `LoopResources` 是否落在本 ADR 推荐域
- [ ] 鉴权 / 持久化 / 集群控制是否隔离
- [ ] 是否存在 EL 阻塞红线
- [ ] 压测下 EL pending 与各池队列是否符合预期

差异记录到新的 ADR 或「对标报告」，而不是直接改本参考文档的决策语义。

---

## 12. 参考

- Reactor Core：`Schedulers` 文档（`parallel` / `boundedElastic` / `single` 语义）
- Reactor Netty：`LoopResources`、连接线程模型
- MQTT 3.1.1 / 5.0：CONNECT Session Taken Over、QoS 状态机、Shared Subscription、Will Delay
- 通用原则：不要阻塞 Event Loop（Netty 性能模型）
