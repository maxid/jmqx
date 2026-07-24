package plus.jmqx.broker.concurrent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import plus.jmqx.broker.mqtt.context.ContextHolder;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scheduler 回流与集群池辅助测试
 */
class SchedulerTasksTest {

    private Scheduler publishScheduler;
    private Scheduler clusterScheduler;

    @AfterEach
    void tearDown() {
        if (publishScheduler != null) {
            publishScheduler.dispose();
        }
        if (clusterScheduler != null) {
            clusterScheduler.dispose();
        }
    }

    @Test
    void scheduleRunsOnTargetSchedulerNotCallerThread() throws Exception {
        publishScheduler = Schedulers.newSingle("jmqx-publish-test");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();
        String caller = Thread.currentThread().getName();

        // 模拟 ACL Offload 回调线程
        Thread offload = new Thread(() -> SchedulerTasks.schedule(publishScheduler, () -> {
            threadName.set(Thread.currentThread().getName());
            latch.countDown();
        }), "jmqx-acl-io-simulated");
        offload.start();

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertNotNull(threadName.get());
        assertTrue(threadName.get().startsWith("jmqx-publish-test"), threadName.get());
        assertTrue(!threadName.get().equals(caller));
        assertTrue(!threadName.get().contains("acl-io"));
    }

    @Test
    void subscribeOnClusterUsesDedicatedPool() throws Exception {
        clusterScheduler = Schedulers.newSingle("jmqx-cluster-test");
        ContextHolder holder = ContextHolder.builder().clusterScheduler(clusterScheduler).build();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        SchedulerTasks.subscribeOnCluster(holder, Mono.fromRunnable(() -> {
                    threadName.set(Thread.currentThread().getName());
                    latch.countDown();
                }))
                .subscribe();

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertTrue(threadName.get().startsWith("jmqx-cluster-test"), threadName.get());
    }

}
