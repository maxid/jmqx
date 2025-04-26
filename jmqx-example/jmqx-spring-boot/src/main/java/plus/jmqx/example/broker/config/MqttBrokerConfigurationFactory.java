package plus.jmqx.example.broker.config;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.stereotype.Component;
import plus.jmqx.broker.mqtt.MqttConfiguration;

/**
 * MQTT服务配置
 *
 * @author maxid
 * @since 2025/4/26 10:52
 */
@Data
@Component
@EqualsAndHashCode(callSuper = true)
public class MqttBrokerConfigurationFactory
        extends AbstractFactoryBean<MqttConfiguration> implements FactoryBean<MqttConfiguration> {
    @Override
    public Class<?> getObjectType() {
        return MqttConfiguration.class;
    }

    @Override
    protected MqttConfiguration createInstance() throws Exception {
        MqttConfiguration config = new MqttConfiguration();
        return config;
    }
}
