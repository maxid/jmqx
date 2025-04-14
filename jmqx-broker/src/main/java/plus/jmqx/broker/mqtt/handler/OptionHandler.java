package plus.jmqx.broker.mqtt.handler;

import io.netty.channel.ChannelOption;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import reactor.netty.tcp.TcpServer;

import java.util.Map;

/**
 * TcpServer 配置
 *
 * @author maxid
 * @since 2025/4/10 16:07
 */
public class OptionHandler {
    protected TcpServer server;

    public TcpServer initTcpServer(MqttConfiguration config) {
        this.server = TcpServer.create();
        if (config.getOptions() != null) {
            for (Map.Entry<String, Object> entry : config.getOptions().entrySet()) {
                this.server = this.server.option(ChannelOption.valueOf(entry.getKey()), entry.getValue());
            }
        }
        if (config.getChildOptions() != null) {
            for (Map.Entry<String, Object> entry : config.getChildOptions().entrySet()) {
                this.server = this.server.childOption(ChannelOption.valueOf(entry.getKey()), entry.getValue());
            }
        }
        return this.server;
    }
}
