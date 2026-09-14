package plus.jmqx.broker.mqtt.message.dispatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import plus.jmqx.broker.mqtt.channel.MqttSession;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PINGREQ 心跳回调：PONG 之后在 dispatchScheduler 上触发 {@code onPing}。
 */
class PingReqHandlerTest {

    private Scheduler dispatchScheduler;

    @AfterEach
    void tearDown() {
        if (dispatchScheduler != null) {
            dispatchScheduler.dispose();
        }
    }

    /**
     * {@code onPing} 必须带着会话 clientId/username，且不在调用线程执行。
     */
    @Test
    void dispatchOnPingRunsOnDispatchSchedulerWithSessionIdentity() throws Exception {
        dispatchScheduler = Schedulers.newSingle("jmqx-dispatch-test");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<PingMessage> captured = new AtomicReference<>();
        AtomicReference<String> threadName = new AtomicReference<>();
        String caller = Thread.currentThread().getName();

        PlatformDispatcher dispatcher = new PlatformDispatcher() {
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onPing(PingMessage message) {
                return Mono.fromRunnable(() -> {
                    captured.set(message);
                    threadName.set(Thread.currentThread().getName());
                    latch.countDown();
                });
            }
        };

        MqttSession session = new MqttSession();
        session.setClientId("dev-1");
        session.setUsername("user-1");

        ContextHolder holder = ContextHolder.builder()
                .platformDispatcher(dispatcher)
                .dispatchScheduler(dispatchScheduler)
                .build();

        PingReqHandler.dispatchOnPing(session, holder);

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertNotNull(captured.get());
        assertEquals("dev-1", captured.get().getClientId());
        assertEquals("user-1", captured.get().getUsername());
        assertTrue(threadName.get().startsWith("jmqx-dispatch-test"), threadName.get());
        assertTrue(!threadName.get().equals(caller));
    }

    /**
     * 未覆盖 {@code onPing} 的实现走默认空实现，不应抛错。
     */
    @Test
    void defaultOnPingIsNoopSoExistingDispatchersKeepCompiling() {
        PlatformDispatcher dispatcher = new PlatformDispatcher() {
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.empty();
            }
        };
        dispatcher.onPing(PingMessage.builder().clientId("c").username("u").build()).block();
    }
}
