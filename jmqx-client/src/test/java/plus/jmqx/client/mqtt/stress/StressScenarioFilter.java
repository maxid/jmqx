package plus.jmqx.client.mqtt.stress;

/**
 * 按 {@code -Djmqx.client.stress.scenario=} 过滤压测场景。
 *
 * <p>可选值：{@code connect}、{@code publish}、{@code subscribe}、{@code all}（默认，运行全部）。
 */
public final class StressScenarioFilter {

    private StressScenarioFilter() {
    }

    public static boolean isEnabled(String scenario) {
        String selected = System.getProperty("jmqx.client.stress.scenario", "all");
        if (selected == null || selected.isEmpty() || "all".equalsIgnoreCase(selected)) {
            return true;
        }
        return scenario.equalsIgnoreCase(selected);
    }

    public static boolean connectEnabled() {
        return isEnabled("connect");
    }

    public static boolean publishEnabled() {
        return isEnabled("publish");
    }

    public static boolean subscribeEnabled() {
        return isEnabled("subscribe");
    }

}
