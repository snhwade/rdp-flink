package com.riskplatform.flink.config;

/**
 * 与 indicator-store / rule-config 一致的切片粒度命名。
 */
public final class SliceGranularityNames {

    private SliceGranularityNames() {
    }

    public static long stepSeconds(String granularity) {
        if (granularity == null || granularity.isBlank()) {
            return 86400L;
        }
        return switch (granularity.trim().toUpperCase()) {
            case "MINUTE" -> 60L;
            case "HOUR" -> 3600L;
            case "DAY" -> 86400L;
            default -> throw new IllegalArgumentException("未知 sliceGranularity: " + granularity);
        };
    }

    public static String fromSeconds(long sliceSeconds) {
        if (sliceSeconds == 60L) {
            return "MINUTE";
        }
        if (sliceSeconds == 3600L) {
            return "HOUR";
        }
        return "DAY";
    }

    /** 窗口天数 → 窗口内切片个数（与 indicator-store TTL 语义一致）。 */
    public static int windowSlices(int windowDays, long sliceSeconds) {
        int days = Math.max(1, windowDays);
        return Math.max(1, (int) ((long) days * 86400L / sliceSeconds));
    }
}
