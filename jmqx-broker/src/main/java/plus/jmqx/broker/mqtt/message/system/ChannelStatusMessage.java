package plus.jmqx.broker.mqtt.message.system;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import plus.jmqx.broker.mqtt.channel.SessionStatus;

/**
 * 会话状态消息
 *
 * @author maxid
 * @since 2025/4/18 09:24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChannelStatusMessage {
    /**
     * 客户端 ID
     */
    private String        clientId;
    /**
     * 消息时间
     */
    private long          timestamp;
    /**
     * 用户名
     */
    private String        username;
    /**
     * 会话状态
     *
     * @see SessionStatus
     */
    private SessionStatus channelStatus;
}
