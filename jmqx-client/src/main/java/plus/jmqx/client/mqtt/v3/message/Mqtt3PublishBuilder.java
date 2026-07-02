package plus.jmqx.client.mqtt.v3.message;

import plus.jmqx.client.mqtt.message.QoS;

/**
 * {@link Mqtt3Publish} 的可变 builder。
 *
 * @author maxid
 */
public class Mqtt3PublishBuilder {

    private String topic;
    private byte[] payload;
    private QoS qos = QoS.AT_MOST_ONCE;
    private boolean retain = false;
    private boolean dup = false;
    private int packetId = 0;

    public Mqtt3PublishBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public Mqtt3PublishBuilder payload(byte[] payload) {
        this.payload = payload;
        return this;
    }

    public Mqtt3PublishBuilder qos(QoS qos) {
        this.qos = qos;
        return this;
    }

    public Mqtt3PublishBuilder retain(boolean retain) {
        this.retain = retain;
        return this;
    }

    public Mqtt3PublishBuilder dup(boolean dup) {
        this.dup = dup;
        return this;
    }

    public Mqtt3PublishBuilder packetId(int packetId) {
        this.packetId = packetId;
        return this;
    }

    public Mqtt3Publish build() {
        return new Mqtt3PublishImpl(topic, payload, qos, retain, dup, packetId);
    }
}
