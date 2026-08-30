package com.riskplatform.flink.model;

import java.io.Serializable;
import java.util.List;

/**
 * Flink 作业使用的指标定义（与 rule-config / indicator-store 对齐）。
 */
public record IndicatorDef(
        String refName,
        List<String> eventTypeCodes,
        List<String> dimensions,
        long sliceSeconds,
        int windowSlices,
        String sliceGranularity,
        String accScript) implements Serializable {

    public IndicatorDef {
        if (dimensions != null) {
            dimensions = new java.util.ArrayList<>(dimensions);
        }
        if (eventTypeCodes != null) {
            eventTypeCodes = new java.util.ArrayList<>(eventTypeCodes);
        }
        if (sliceGranularity == null || sliceGranularity.isBlank()) {
            sliceGranularity = "DAY";
        } else {
            sliceGranularity = sliceGranularity.trim().toUpperCase();
        }
    }

    /** 测试/兼容构造：不限事件类型。 */
    public static IndicatorDef forTest(String refName, List<String> dimensions,
                                      long sliceSeconds, int windowSlices, String accScript) {
        return new IndicatorDef(refName, List.of(), dimensions, sliceSeconds, windowSlices,
                com.riskplatform.flink.config.SliceGranularityNames.fromSeconds(sliceSeconds), accScript);
    }

    public boolean matchesEvent(String eventTypeCode) {
        if (eventTypeCode == null || eventTypeCode.isBlank()) {
            return false;
        }
        return eventTypeCodes == null || eventTypeCodes.isEmpty()
                || eventTypeCodes.contains(eventTypeCode);
    }
}
