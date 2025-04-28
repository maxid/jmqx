package plus.jmqx.example.broker.dispatch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import plus.jmqx.broker.mqtt.message.dispatch.ConnectMessage;

/**
 * 设备上线处理
 *
 * @author maxid
 * @since 2025/4/28 09:57
 */
@Slf4j
@Component
public class ConnectionProcessor {

    public void process(ConnectMessage message) {
        log.info("【设备上线】{}", message);
    }

}
