package plus.jmqx.broker.auth;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * 鉴权执行器<br/>
 * 因为不清楚用户的鉴权实现采用何种方案(如 openfeign 等), 可能会导致 Netty event loop 被阻塞, 连带影响心跳、收发包和重连风暴<br/>
 * 统一把鉴权调用切到业务线程池（如 boundedElastic / 自定义线程池），并设置超时和熔断。
 *
 * @author maxid
 * @since 2026/4/16 23:04
 */
public class AuthExecutor {

    private final AuthManager authManager;
    private final Scheduler scheduler;
    private final long timeoutMillis;

    public AuthExecutor(AuthManager authManager, Scheduler scheduler, long timeoutMillis) {
        this.authManager = authManager;
        this.scheduler = scheduler == null ? Schedulers.boundedElastic() : scheduler;
        this.timeoutMillis = Math.max(timeoutMillis, 1L);
    }

    /**
     * 执行鉴权并在超时/异常场景下返回失败
     *
     * @param clientId 设备 ID
     * @param username 用户名
     * @param password 密码
     * @return 鉴权结果
     */
    public Mono<Boolean> execute(String clientId, String username, byte[] password) {
        Mono<Boolean> source = authManager instanceof AsyncAuthManager asyncAuthManager
                ? asyncAuthManager.authAsync(clientId, username, password)
                : Mono.fromCallable(() -> authManager.auth(clientId, username, password))
                .subscribeOn(scheduler);
        return source
                .timeout(Duration.ofMillis(timeoutMillis))
                .onErrorReturn(Boolean.FALSE)
                .defaultIfEmpty(Boolean.FALSE);
    }

}
