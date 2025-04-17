package plus.jmqx.broker.mqtt.transport.handler;

import cn.hutool.core.util.StrUtil;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.config.Configuration;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import reactor.netty.tcp.SslProvider;
import reactor.netty.tcp.TcpServer;

import java.io.File;

/**
 * SSL 配置处理
 *
 * @author maxid
 * @since 2025/4/9 11:46
 */
@Slf4j
public class SslHandler extends OptionHandler {

    @Override
    public TcpServer initTcpServer(MqttConfiguration config) {
        this.server = super.initTcpServer(config);
        if (config.getSslEnable()) {
            this.server = this.server.secure(spec -> this.secure(spec, config));
        }
        return this.server;
    }

    private void secure(SslProvider.SslContextSpec spec, Configuration config) {
        try {
            if (config.getSslEnable()) {
                SslContextBuilder builder;
                if (StrUtil.isNotEmpty(config.getSslCa())) {
                    builder = SslContextBuilder.forServer(new File(config.getSslCrt()), new File(config.getSslKey()));
                    builder = builder.trustManager(new File(config.getSslCa()));
                } else {
                    SelfSignedCertificate ssc = new SelfSignedCertificate();
                    builder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
                }
                spec.sslContext(builder.build());
            }
        } catch (Exception e) {
            log.error("ssl read error", e);
        }
    }
}
