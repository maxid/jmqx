package plus.jmqx.broker.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Jackson 消息序列化、反序列化配置
 *
 * @author maxid
 * @since 2025/4/22 09:56
 */
public class JacksonMessageCodec implements MessageCodec {
    private final ObjectMapper delegate;

    public JacksonMessageCodec() {
        this(DefaultObjectMapper.OBJECT_MAPPER);
    }

    public JacksonMessageCodec(ObjectMapper delegate) {
        this.delegate = delegate;
    }

    @Override
    public Message deserialize(InputStream stream) throws Exception {
        return this.delegate.readValue(stream, Message.class);
    }

    @Override
    public void serialize(Message message, OutputStream stream) throws Exception {
        this.delegate.writeValue(stream, message);
    }
}
