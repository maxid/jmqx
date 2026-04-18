package plus.jmqx.broker.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 鉴权执行器单元测试
 *
 * @author maxid
 * @since 2026/4/16 23:04
 */
class AuthExecutorTest {

    @Test
    void executeSupportsSyncAuthManager() {
        AuthManager authManager = (clientId, username, password) -> true;
        AuthExecutor executor = new AuthExecutor(authManager, "mqtt", 1000, 8, 1000);
        Boolean passed = executor.execute("c1", "u1", new byte[]{1}).join();
        assertEquals(Boolean.TRUE, passed);
    }

    @Test
    void executeReturnsFalseWhenAuthException() {
        AuthManager authManager = (clientId, username, password) -> {
            throw new IllegalStateException("auth error");
        };
        AuthExecutor executor = new AuthExecutor(authManager,"mqtt", 1000, 8, 1000);
        Boolean passed = executor.execute("c1", "u1", new byte[]{1}).join();
        assertEquals(Boolean.FALSE, passed);
    }

    @Test
    void executeReturnsFalseWhenAuthTimeout() {
        AuthManager authManager = (clientId, username, password) -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        };
        AuthExecutor executor = new AuthExecutor(authManager, "mqtt", 50, 8, 1000);
        Boolean passed = executor.execute("c1", "u1", new byte[]{1}).join();
        assertNotEquals(Boolean.TRUE, passed);
    }

}
