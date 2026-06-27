package plus.jmqx.broker.util;

/**
 * 令牌桶速率限制器
 * <p>
 * 用于控制连接速率等场景。线程安全。
 * </p>
 *
 * @author maxid
 * @since 2025/4/16
 */
public class TokenBucketRateLimiter {

    private final long capacity;
    private final long tokensPerSecond;
    private long tokens;
    private long lastRefillNanos;

    /**
     * 构造令牌桶
     *
     * @param tokensPerSecond 每秒产生的令牌数
     */
    public TokenBucketRateLimiter(long tokensPerSecond) {
        this.capacity = tokensPerSecond;
        this.tokensPerSecond = tokensPerSecond;
        this.tokens = tokensPerSecond;
        this.lastRefillNanos = System.nanoTime();
    }

    /**
     * 尝试获取一个令牌
     *
     * @return true 获取成功，false 超过速率限制
     */
    public synchronized boolean tryAcquire() {
        refill();
        if (tokens > 0) {
            tokens--;
            return true;
        }
        return false;
    }

    /**
     * 补充令牌
     */
    private void refill() {
        long now = System.nanoTime();
        long elapsed = now - lastRefillNanos;
        // 每秒补充 tokensPerSecond 个令牌
        long added = (long) (elapsed * tokensPerSecond / 1_000_000_000.0);
        if (added > 0) {
            tokens = Math.min(capacity, tokens + added);
            lastRefillNanos = now;
        }
    }

}
