package plus.jmqx.broker.mqtt.message.dispatch;

import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.netty.Connection;

/**
 * PINGREQ：EventLoop 上回 PONG，再在 dispatchScheduler 上回调平台 {@code onPing}。
 *
 * @author maxid
 * @since 2026/9/14
 */
public final class PingReqHandler {

    private PingReqHandler() {
    }

    /**
     * 先写 PINGRESP，再分发 onPing。
     *
     * @param session 会话
     * @param holder  全局上下文
     */
    public static void pongAndDispatch(MqttSession session, ContextHolder holder) {
        writePong(session);
        dispatchOnPing(session, holder);
    }

    /**
     * 在 dispatchScheduler 上回调 {@link PlatformDispatcher#onPing(PingMessage)}。
     *
     * @param session 会话
     * @param holder  全局上下文
     */
    public static void dispatchOnPing(MqttSession session, ContextHolder holder) {
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

    static void writePong(MqttSession session) {
        Connection connection = session.getConnection();
        if (connection == null || connection.isDisposed()) {
            return;
        }
        connection.outbound().sendObject(Mono.just(MqttMessageBuilder.pongMessage())).then().subscribe();
    }
}
