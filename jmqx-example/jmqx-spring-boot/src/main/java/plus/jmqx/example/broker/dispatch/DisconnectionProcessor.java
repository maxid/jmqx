package plus.jmqx.example.broker.dispatch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import plus.jmqx.broker.mqtt.message.dispatch.ConnectionLostMessage;

/**
 * 设备离线处理
 *
 * @author maxid
 * @since 2025/4/28 09:59
 */
@Slf4j
@Component
public class DisconnectionProcessor {

    public void process(ConnectionLostMessage message) {
        log.info("【设备离线】{}", message);
    }

}
