package plus.jmqx.client.mqtt.internal;

import plus.jmqx.client.mqtt.message.MqttPublish;

/**
 * 入站投递 + 背压（完整实现见 Task 17）。当前为桩：立即投递 + 立即 ack。
 *
 * @author maxid
 */
public final class MqttInbox {

    public void deliver(MqttPublish pub, Runnable ackAction) {
        try {
            ackAction.run();
        } catch (Exception ignored) {
        }
    }
}
