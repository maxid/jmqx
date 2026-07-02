package plus.jmqx.client.mqtt.internal.util;

/**
 * MQTT 主题过滤器匹配（spec §4.7），支持 '+'（单层）与 '#'（多层）通配符。
 *
 * <p>通配符必须占据完整的一层（'/' 分隔）。
 *
 * @author maxid
 */
public final class TopicMatcher {

    private TopicMatcher() {
    }

    public static boolean matches(String filter, String topic) {
        if (filter == null || topic == null) {
            return false;
        }
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        int fi = 0;
        for (int ti = 0; ti < t.length; ti++) {
            if (fi >= f.length) {
                return false;
            }
            String level = f[fi];
            if ("#".equals(level)) {
                // '#' 必须是最后一层；若其后仍有层，则过滤器非法，不匹配
                return fi == f.length - 1;
            }
            if ("+".equals(level) || level.equals(t[ti])) {
                fi++;
                continue;
            }
            return false;
        }
        // topic 已耗尽；仅当 filter 也耗尽，或 filter 以 '#' 结尾时匹配
        if (fi == f.length) {
            return true;
        }
        return fi == f.length - 1 && "#".equals(f[fi]);
    }

}
