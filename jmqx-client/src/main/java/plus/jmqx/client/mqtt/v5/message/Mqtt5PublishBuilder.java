package plus.jmqx.client.mqtt.v5.message;

import plus.jmqx.client.mqtt.message.QoS;

/**
 * {@link Mqtt5Publish} 的可变 builder。
 *
 * @author maxid
 */
public class Mqtt5PublishBuilder {

    private String topic;
    private byte[] payload;
    private QoS qos = QoS.AT_MOST_ONCE;
    private boolean retain;
    private boolean dup;
    private int packetId;
    private Mqtt5PublishProperties properties = Mqtt5PublishProperties.builder().build();

    public Mqtt5PublishBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public Mqtt5PublishBuilder payload(byte[] payload) {
        this.payload = payload;
        return this;
    }

    public Mqtt5PublishBuilder qos(QoS qos) {
        this.qos = qos;
        return this;
    }

    public Mqtt5PublishBuilder retain(boolean retain) {
        this.retain = retain;
        return this;
    }

    public Mqtt5PublishBuilder dup(boolean dup) {
        this.dup = dup;
        return this;
    }

    public Mqtt5PublishBuilder packetId(int packetId) {
        this.packetId = packetId;
        return this;
    }

    public Mqtt5PublishBuilder properties(Mqtt5PublishProperties properties) {
        this.properties = properties;
        return this;
    }

    public Mqtt5Publish build() {
        return new Mqtt5PublishImpl(topic, payload, qos, retain, dup, packetId, properties);
    }
}
