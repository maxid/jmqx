package plus.jmqx.client.mqtt.internal.util;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PacketIdManagerTest {

    @Test
    void rangeIs1to65535() {
        PacketIdManager pm = new PacketIdManager();
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 65535; i++) {
            int id = pm.nextPacketId();
            assertTrue(id >= 1 && id <= 65535, "out of range: " + id);
            assertTrue(seen.add(id), "duplicate within one cycle: " + id);
        }
    }

    @Test
    void wrapsAroundAfter65535() {
        PacketIdManager pm = new PacketIdManager();
        for (int i = 0; i < 65535; i++) {
            pm.nextPacketId();
        }
        int next = pm.nextPacketId();
        assertTrue(next >= 1 && next <= 65535);
    }

    @Test
    void neverReturnsZero() {
        PacketIdManager pm = new PacketIdManager();
        for (int i = 0; i < 200_000; i++) {
            assertNotEquals(0, pm.nextPacketId(), "returned 0 at iteration " + i);
        }
    }

    @Test
    void concurrentNoZero() throws InterruptedException {
        PacketIdManager pm = new PacketIdManager();
        int threads = 8, perThread = 10_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Set<Integer> all = java.util.Collections.synchronizedSet(new HashSet<>());
        CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                for (int j = 0; j < perThread; j++) {
                    all.add(pm.nextPacketId());
                }
                latch.countDown();
            });
        }
        latch.await();
        pool.shutdown();
        // 80000 ids, range 1..65535 wraps ~1.2x; uniqueness only holds within a single cycle,
        // so assert no id is 0 and set non-empty (true uniqueness is per-cycle, not global after wrap).
        assertFalse(all.contains(0));
        assertFalse(all.isEmpty());
    }
}
