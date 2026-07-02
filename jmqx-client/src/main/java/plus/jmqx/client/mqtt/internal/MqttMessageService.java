package plus.jmqx.client.mqtt.internal;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.message.MqttConnAck;
import plus.jmqx.client.mqtt.message.MqttPublish;
import plus.jmqx.client.mqtt.message.MqttSubAck;
import plus.jmqx.client.mqtt.message.MqttSubscribe;
import plus.jmqx.client.mqtt.message.MqttUnsubscribe;

/**
 * 协议适配器 —— 隔离 v3/v5 线路格式差异。
 *
 * <p>引擎（{@code DefaultMqttClient}）只依赖此接口，v3/v5 各提供实现。
 *
 * @author maxid
 */
public interface MqttMessageService {

    /**
     * 编码 CONNECT。config 提供 clientId/keepAlive/auth/will。
     */
    MqttMessage encodeConnect(MqttClientConfig config);

    MqttMessage encodePublish(MqttPublish publish, int packetId, boolean dup);

    MqttMessage encodeSubscribe(MqttSubscribe subscribe);

    MqttMessage encodeUnsubscribe(MqttUnsubscribe unsubscribe);

    MqttMessage encodePubAck(int packetId);

    MqttMessage encodePubRec(int packetId);

    MqttMessage encodePubRel(int packetId);

    MqttMessage encodePubComp(int packetId);

    MqttMessage encodeDisconnect();

    MqttMessage encodePingReq();

    MqttConnAck decodeConnAck(MqttConnAckMessage msg, MqttClientConfig config);

    MqttPublish decodePublish(MqttPublishMessage msg);

    MqttSubAck decodeSubAck(MqttSubAckMessage msg);

    int decodePacketId(MqttMessage msg);

    boolean isConnectionAccepted(MqttConnAck ack);

    RuntimeException connectionRefusedException(MqttConnAck ack);

}
