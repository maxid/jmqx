package plus.jmqx.broker.mqtt.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * jackson工具类
 *
 * @author maxid
 * @since 2025/4/16 14:30
 */
@Slf4j
public class JacksonUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    }

    public static String bean2Json(Object data) {
        try {
            return mapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            log.error("JacksonUtil bean2Json {} error", data, e);
            return "";
        }
    }

    public static <T> T json2Bean(String jsonData, Class<T> beanType) {
        try {
            return mapper.readValue(jsonData, beanType);
        } catch (Exception e) {
            log.error("JacksonUtil json {}  error", jsonData, e);
            return null;
        }
    }

    public static <T> List<T> json2List(String jsonData, Class<T> beanType) {
        try {
            JavaType javaType = mapper.getTypeFactory().constructParametricType(List.class, beanType);
            return mapper.readValue(jsonData, javaType);
        } catch (Exception e) {
            log.error("JacksonUtil json2List error", e);
            return Collections.emptyList();
        }
    }

    public static <K, V> Map<K, V> json2Map(String jsonData, Class<K> keyType, Class<V> valueType) {
        if (jsonData == null || "".equals(jsonData)) {
            return Collections.emptyMap();
        } else {
            try {
                JavaType javaType = mapper.getTypeFactory().constructMapType(Map.class, keyType, valueType);
                return mapper.readValue(jsonData, javaType);
            } catch (Exception e) {
                log.error("JacksonUtil json2Map error", e);
                return Collections.emptyMap();
            }
        }
    }

    public static <K, V> String map2Json(Map<K, V> map) {
        try {
            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            log.error("JacksonUtil map2Json error", e);
            return "";
        }
    }

    public static Object dynamic(String s) {
        if (s.startsWith("{") && s.endsWith("}")) {
            return JacksonUtil.json2Map(s, String.class, Object.class);
        } else if (s.startsWith("[") && s.endsWith("]")) {
            return json2List(s, Map.class);
        } else {
            return s;
        }
    }

    public static String dynamicJson(Object object) {
        if (object instanceof String) {
            return String.valueOf(object);
        } else {
            return JacksonUtil.bean2Json(object);
        }
    }
}