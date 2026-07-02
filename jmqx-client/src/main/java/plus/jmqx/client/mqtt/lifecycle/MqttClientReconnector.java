package plus.jmqx.client.mqtt.lifecycle;

import lombok.Getter;

/**
 * 重连控制器，传入 {@link MqttClientDisconnectedListener}。
 *
 * <p>监听器通过修改此对象来影响重连行为（是否重连、延迟、是否重新订阅）。
 * 引擎在所有监听器执行完毕后读取其状态。
 *
 * @author maxid
 * @since 1.4.14
 */
@Getter
public final class MqttClientReconnector {

    /**
     * 是否继续重连
     */
    private boolean reconnect;
    /**
     * 下次重连前的延迟（毫秒）
     */
    private long    delayMs                           = 0;
    /**
     * 当前已重连次数
     */
    private int     attempts;
    /**
     * 会话仍存在时是否重新订阅
     */
    private boolean resubscribeIfSessionPresent       = false;
    /**
     * 会话过期时是否重新订阅
     */
    private boolean resubscribeIfSessionExpired       = true;
    /**
     * 会话过期时是否重发离线缓存
     */
    private boolean republishBufferedIfSessionExpired = true;

    /**
     * 初始化重连控制器
     *
     * @param attempts  当前已重连次数
     * @param reconnect 是否继续重连
     */
    public MqttClientReconnector(int attempts, boolean reconnect) {
        this.attempts = attempts;
        this.reconnect = reconnect;
    }

    /**
     * 设置是否继续重连
     *
     * @param reconnect 是否继续重连
     * @return this reconnector
     */
    public MqttClientReconnector reconnect(boolean reconnect) {
        this.reconnect = reconnect;
        return this;
    }

    /**
     * 设置下次重连前的延迟（毫秒）
     *
     * @param delayMs 下次重连前的延迟（毫秒）
     * @return this reconnector
     */
    public MqttClientReconnector delay(long delayMs) {
        this.delayMs = delayMs;
        return this;
    }

    /**
     * 设置当前已重连次数
     *
     * @param attempts 当前已重连次数
     */
    public MqttClientReconnector setAttempts(int attempts) {
        this.attempts = attempts;
        return this;
    }

    /**
     * 设置会话仍存在时是否重新订阅
     *
     * @param v 会话仍存在时是否重新订阅
     * @return this reconnector
     */
    public MqttClientReconnector resubscribeIfSessionPresent(boolean v) {
        this.resubscribeIfSessionPresent = v;
        return this;
    }

    /**
     * 设置会话过期时是否重新订阅
     *
     * @param v 会话过期时是否重新订阅
     * @return this reconnector
     */
    public MqttClientReconnector resubscribeIfSessionExpired(boolean v) {
        this.resubscribeIfSessionExpired = v;
        return this;
    }

    /**
     * 设置会话过期时是否重发离线缓存中的消息
     *
     * @param v 会话过期时是否重发离线缓存中的消息
     * @return this reconnector
     */
    public MqttClientReconnector republishBufferedIfSessionExpired(boolean v) {
        this.republishBufferedIfSessionExpired = v;
        return this;
    }

}
