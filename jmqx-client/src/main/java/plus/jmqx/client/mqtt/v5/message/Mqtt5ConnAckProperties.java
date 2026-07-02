package plus.jmqx.client.mqtt.v5.message;

import lombok.Builder;
import lombok.Value;

/**
 * MQTT 5.0 CONNACK 属性。
 *
 * @author maxid
 */
@Value
@Builder
public class Mqtt5ConnAckProperties {

    /**
     * Receive Maximum
     */
    int     receiveMaximum;
    /**
     * 服务端 Keep Alive 间隔（秒）
     */
    int     serverKeepAlive;
    /**
     * 会话过期时间（秒）
     */
    long    sessionExpiryInterval;
    /**
     * 响应信息
     */
    String  responseInformation;
    /**
     * 服务端引用
     */
    String  serverReference;
    /**
     * 服务端分配的客户端标识符
     */
    String  assignedClientIdentifier;
    /**
     * 是否设置了最大报文大小
     */
    boolean maximumPacketSizePresent;
    /**
     * 最大报文大小
     */
    int     maximumPacketSize;

}
