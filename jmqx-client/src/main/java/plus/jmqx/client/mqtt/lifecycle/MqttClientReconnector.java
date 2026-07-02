package plus.jmqx.client.mqtt.lifecycle;

/**
 * 重连控制器，传入 {@link MqttClientDisconnectedListener}。
 *
 * <p>监听器通过修改此对象来影响重连行为（是否重连、延迟、是否重新订阅）。引擎在所有监听器执行完毕后读取其状态。
 *
 * @author maxid
 */
public final class MqttClientReconnector {

    private boolean reconnect;
    private long delayMs = 0;
    private int attempts;
    private boolean resubscribeIfSessionPresent = false;
    private boolean resubscribeIfSessionExpired = true;
    private boolean republishBufferedIfSessionExpired = true;

    public MqttClientReconnector(int attempts, boolean reconnect) {
        this.attempts = attempts;
        this.reconnect = reconnect;
    }

    public MqttClientReconnector reconnect(boolean reconnect) {
        this.reconnect = reconnect;
        return this;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public MqttClientReconnector delay(long delayMs) {
        this.delayMs = delayMs;
        return this;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public MqttClientReconnector resubscribeIfSessionPresent(boolean v) {
        this.resubscribeIfSessionPresent = v;
        return this;
    }

    public boolean isResubscribeIfSessionPresent() {
        return resubscribeIfSessionPresent;
    }

    public MqttClientReconnector resubscribeIfSessionExpired(boolean v) {
        this.resubscribeIfSessionExpired = v;
        return this;
    }

    public boolean isResubscribeIfSessionExpired() {
        return resubscribeIfSessionExpired;
    }

    public MqttClientReconnector republishBufferedIfSessionExpired(boolean v) {
        this.republishBufferedIfSessionExpired = v;
        return this;
    }

    public boolean isRepublishBufferedIfSessionExpired() {
        return republishBufferedIfSessionExpired;
    }
}
