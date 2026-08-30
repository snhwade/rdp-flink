package com.riskplatform.flink.config;

import com.riskplatform.flink.model.IndicatorDef;

import java.util.ArrayList;
import java.util.List;

/** 内置兜底指标（rule-config 不可达且无快照时使用）。 */
public final class BootstrapIndicatorDefinitions {

    private BootstrapIndicatorDefinitions() {
    }

    public static List<IndicatorDef> defaults() {
        List<IndicatorDef> defs = new ArrayList<>();
        defs.add(new IndicatorDef(
                "txn_cnt_1d",
                List.of("EVT_PAY_RESULT"),
                List.of("merchantId"),
                86400L,
                1,
                "DAY",
                "current + 1"));
        defs.add(new IndicatorDef(
                "txn_amount_1d",
                List.of("EVT_PAY_RESULT"),
                List.of("merchantId"),
                86400L,
                1,
                "DAY",
                "current + amount"));
        return defs;
    }
}
