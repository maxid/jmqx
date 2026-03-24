package plus.jmqx.example.broker;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;
import plus.jmqx.broker.Bootstrap;
import plus.jmqx.broker.acl.AclManager;
import plus.jmqx.broker.auth.AuthManager;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher;

/**
 * MQTT Broker
 *
 * @author maxid
 * @since 2025/4/26 10:38
 */
@Service
@RequiredArgsConstructor
public class PlatformMqttBroker implements ApplicationRunner {

    private final MqttConfiguration  config;
    private final AuthManager        authManager;
    private final AclManager         aclManager;
    private final PlatformDispatcher dispatcher;

    /**
     * 应用启动后运行。
     *
     * @param args 启动参数
     * @throws Exception 启动异常
     */
    @Override
    public void run(ApplicationArguments args) throws Exception {
        start();
    }

    /**
     * 启动 MQTT Broker。
     *
     * @throws Exception 启动异常
     */
    private void start() throws Exception {
        Bootstrap bootstrap = new Bootstrap(config, aclManager, authManager, dispatcher);
        // bootstrap.start().block();
        bootstrap.startAwait();
    }
}
