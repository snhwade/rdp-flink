package com.riskplatform.flink.config;

import com.riskplatform.flink.model.IndicatorDef;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RuleConfigDefinitionLoaderTest {

    @Test
    void fromApiRow_mapsRuleConfigFields() {
        IndicatorDef def = RuleConfigDefinitionLoader.fromApiRow(Map.of(
                "refName", "b2b_daily_amt",
                "eventTypeCodes", List.of("EVT_B2B_RECV"),
                "dimensions", List.of("merchantId"),
                "windowDays", 1,
                "sliceGranularity", "DAY",
                "accScript", "current + amount",
                "status", "ONLINE"));

        assertThat(def.refName()).isEqualTo("b2b_daily_amt");
        assertThat(def.eventTypeCodes()).containsExactly("EVT_B2B_RECV");
        assertThat(def.dimensions()).containsExactly("merchantId");
        assertThat(def.sliceSeconds()).isEqualTo(86400L);
        assertThat(def.windowSlices()).isEqualTo(1);
        assertThat(def.sliceGranularity()).isEqualTo("DAY");
        assertThat(def.accScript()).isEqualTo("current + amount");
        assertThat(def.matchesEvent("EVT_B2B_RECV")).isTrue();
        assertThat(def.matchesEvent("OTHER")).isFalse();
    }

    @Test
    void windowSlices_scalesWithHourGranularity() {
        IndicatorDef def = RuleConfigDefinitionLoader.fromApiRow(Map.of(
                "refName", "hourly_cnt",
                "eventTypeCodes", List.of("EVT_PAY"),
                "dimensions", List.of("userId"),
                "windowDays", 1,
                "sliceGranularity", "HOUR",
                "accScript", "current + 1"));

        assertThat(def.sliceSeconds()).isEqualTo(3600L);
        assertThat(def.windowSlices()).isEqualTo(24);
        assertThat(def.sliceGranularity()).isEqualTo("HOUR");
    }
}
