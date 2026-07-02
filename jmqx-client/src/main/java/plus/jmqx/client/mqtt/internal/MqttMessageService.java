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
     * 编码 CONNECT 报文。config 提供 clientId/keepAlive/auth/will。
     *
     * @param config 客户端配置
     * @return 编码后的 CONNECT 消息
     */
    MqttMessage encodeConnect(MqttClientConfig config);

    /**
     * 编码 PUBLISH 报文。
     *
     * @param publish  发布消息
     * @param packetId packetId（QoS0 传 0）
     * @param dup      是否为重传
     * @return 编码后的 PUBLISH 消息
     */
    MqttMessage encodePublish(MqttPublish publish, int packetId, boolean dup);

    /**
     * 编码 SUBSCRIBE 报文。
     *
     * @param subscribe 订阅消息
     * @return 编码后的 SUBSCRIBE 消息
     */
    MqttMessage encodeSubscribe(MqttSubscribe subscribe);

    /**
     * 编码 UNSUBSCRIBE 报文。
     *
     * @param unsubscribe 取消订阅消息
     * @return 编码后的 UNSUBSCRIBE 消息
     */
    MqttMessage encodeUnsubscribe(MqttUnsubscribe unsubscribe);

    /**
     * 编码 PUBACK 报文。
     *
     * @param packetId 对应的 packetId
     * @return 编码后的 PUBACK 消息
     */
    MqttMessage encodePubAck(int packetId);

    /**
     * 编码 PUBREC 报文。
     *
     * @param packetId 对应的 packetId
     * @return 编码后的 PUBREC 消息
     */
    MqttMessage encodePubRec(int packetId);

    /**
     * 编码 PUBREL 报文。
     *
     * @param packetId 对应的 packetId
     * @return 编码后的 PUBREL 消息
     */
    MqttMessage encodePubRel(int packetId);

    /**
     * 编码 PUBCOMP 报文。
     *
     * @param packetId 对应的 packetId
     * @return 编码后的 PUBCOMP 消息
     */
    MqttMessage encodePubComp(int packetId);

    /**
     * 编码 DISCONNECT 报文。
     *
     * @return 编码后的 DISCONNECT 消息
     */
    MqttMessage encodeDisconnect();

    /**
     * 编码 PINGREQ 报文。
     *
     * @return 编码后的 PINGREQ 消息
     */
    MqttMessage encodePingReq();

    /**
     * 解码 CONNACK 报文。
     *
     * @param msg    Netty CONNACK 消息
     * @param config 客户端配置
     * @return 解码后的 CONNACK
     */
    MqttConnAck decodeConnAck(MqttConnAckMessage msg, MqttClientConfig config);

    /**
     * 解码 PUBLISH 报文。
     *
     * @param msg Netty PUBLISH 消息
     * @return 解码后的 PUBLISH
     */
    MqttPublish decodePublish(MqttPublishMessage msg);

    /**
     * 解码 SUBACK 报文。
     *
     * @param msg Netty SUBACK 消息
     * @return 解码后的 SUBACK
     */
    MqttSubAck decodeSubAck(MqttSubAckMessage msg);

    /**
     * 从消息中解码 packetId。
     *
     * @param msg MQTT 消息
     * @return packetId
     */
    int decodePacketId(MqttMessage msg);

    /**
     * 判断连接是否被接受。
     *
     * @param ack CONNACK 消息
     * @return 若连接被接受返回 true
     */
    boolean isConnectionAccepted(MqttConnAck ack);

    /**
     * 创建连接被拒绝的异常。
     *
     * @param ack CONNACK 消息（含拒绝原因）
     * @return 连接拒绝异常
     */
    RuntimeException connectionRefusedException(MqttConnAck ack);

}
