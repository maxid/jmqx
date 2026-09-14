package plus.jmqx.broker.mqtt.message.dispatch;

import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.netty.Connection;

/**
 * PINGREQ 旁路处理：在 EventLoop 上回 PINGRESP，再把心跳回调切到 dispatchScheduler。
 * <p>
 * 不进入消息 Sink，避免心跳挤占控制/数据通道。
 *
 * @author maxid
 * @since 2026/9/14
 */
public final class PingReqHandler {

    /**
     * 工具类，禁止实例化
     */
    private PingReqHandler() {
    }

    /**
     * 先写 PINGRESP，再分发 {@code onPing}。
     *
     * @param session 当前会话
     * @param holder  命名空间上下文；未注入分发器时只回 PONG
     */
    public static void pongAndDispatch(MqttSession session, ContextHolder holder) {
        writePong(session);
        dispatchOnPing(session, holder);
    }

    /**
     * 在 {@code dispatchScheduler} 上回调 {@link PlatformDispatcher#onPing(PingMessage)}。
     * <p>
     * 未配置调度器时在调用线程订阅，避免空指针。
     *
     * @param session 当前会话
     * @param holder  命名空间上下文
     */
    public static void dispatchOnPing(MqttSession session, ContextHolder holder) {
        // 未注入生命周期订阅器时静默跳过
        if (holder == null || holder.getPlatformDispatcher() == null) {
            return;
        }
        PingMessage ping = PingMessage.builder()
                .clientId(session.getClientId())
                .username(session.getUsername())
                .build();
        Mono<Void> mono = holder.getPlatformDispatcher().onPing(ping);
        Scheduler scheduler = holder.getDispatchScheduler();
        if (scheduler != null) {
            mono = mono.subscribeOn(scheduler);
        }
        mono.subscribe();
    }

    /**
     * 在当前连接出站写入 PINGRESP；连接为空或已关闭则跳过。
     *
     * @param session 当前会话
     */
    static void writePong(MqttSession session) {
        Connection connection = session.getConnection();
        if (connection == null || connection.isDisposed()) {
            return;
        }
        connection.outbound().sendObject(Mono.just(MqttMessageBuilder.pongMessage())).then().subscribe();
    }
}
