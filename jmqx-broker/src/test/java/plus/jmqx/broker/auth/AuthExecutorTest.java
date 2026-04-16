package plus.jmqx.broker.auth;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.*;

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
        AuthExecutor executor = new AuthExecutor(authManager, Schedulers.boundedElastic(), 1000);
        Boolean passed = executor.execute("c1", "u1", new byte[]{1}).block();
        assertEquals(Boolean.TRUE, passed);
    }

    @Test
    void executeSupportsAsyncAuthManager() {
        class AsyncManager implements AuthManager, AsyncAuthManager {
            @Override
            public boolean auth(String clientId, String username, byte[] password) {
                return false;
            }

            @Override
            public Mono<Boolean> authAsync(String clientId, String username, byte[] password) {
                return Mono.just(true);
            }
        }
        AuthExecutor executor = new AuthExecutor(new AsyncManager(), Schedulers.boundedElastic(), 1000);
        Boolean passed = executor.execute("c1", "u1", new byte[]{1}).block();
        assertEquals(Boolean.TRUE, passed);
    }

    @Test
    void executeReturnsFalseWhenAuthTimeout() {
        class TimeoutManager implements AuthManager, AsyncAuthManager {
            @Override
            public boolean auth(String clientId, String username, byte[] password) {
                return true;
            }

            @Override
            public Mono<Boolean> authAsync(String clientId, String username, byte[] password) {
                return Mono.never();
            }
        }
        AuthExecutor executor = new AuthExecutor(new TimeoutManager(), Schedulers.boundedElastic(), 30);
        Boolean passed = executor.execute("c1", "u1", new byte[]{1}).block();
        assertNotEquals(Boolean.TRUE, passed);
    }

}
