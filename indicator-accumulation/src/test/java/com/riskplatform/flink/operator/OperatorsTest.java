package com.riskplatform.flink.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskplatform.flink.model.IndicatorDef;
import com.riskplatform.flink.model.OrderFinalState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Flink 算子纯逻辑单元测试（R8.2/R8.4/R8.5）。
 */
class OperatorsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    // —— 反序列化 + 死信（R8.4） ——

    @Test
    void deserialize_validJson_ok() {
        OrderDeserializer des = new OrderDeserializer(mapper);
        String json = "{\"orderId\":\"o1\",\"eventTypeCode\":\"B2B_RECV\",\"eventEpochMs\":1700000000000,"
                + "\"fields\":{\"merchant\":\"M1\",\"amount\":100}}";
        DeserializeResult r = des.deserialize(json);
        assertThat(r.success()).isTrue();
        assertThat(r.order().getOrderId()).isEqualTo("o1");
    }

    @Test
    void deserialize_malformed_routedToDlq() {
        OrderDeserializer des = new OrderDeserializer(mapper);
        DeserializeResult r = des.deserialize("{ not json");
        assertThat(r.success()).isFalse();
        assertThat(r.failReason()).contains("反序列化失败");
    }

    @Test
    void deserialize_missingOrderId_routedToDlq() {
        OrderDeserializer des = new OrderDeserializer(mapper);
        DeserializeResult r = des.deserialize("{\"eventTypeCode\":\"X\"}");
        assertThat(r.success()).isFalse();
        assertThat(r.failReason()).contains("orderId");
    }

    // —— 指标路由（R8.2/R8.4） ——

    @Test
    void route_matchesDefWithAllDimensions() {
        IndicatorDef def = IndicatorDef.forTest("txn_cnt", List.of("merchant"), 3600, 24, "current + 1");
        IndicatorRouter router = new IndicatorRouter(List.of(def));
        OrderFinalState order = new OrderFinalState("o1", "E", 1L, Map.of("merchant", "M1", "amount", 100));
        assertThat(router.route(order)).containsExactly(def);
    }

    @Test
    void route_skipsDefWithMissingDimension() {
        IndicatorDef def = IndicatorDef.forTest("by_country", List.of("country"), 3600, 24, "current + 1");
        IndicatorRouter router = new IndicatorRouter(List.of(def));
        OrderFinalState order = new OrderFinalState("o1", "E", 1L, Map.of("merchant", "M1"));
        assertThat(router.route(order)).isEmpty();
    }

    @Test
    void route_filtersByEventType() {
        IndicatorDef def = new IndicatorDef(
                "cnt", List.of("EVT_PAY"), List.of("merchantId"),
                86400L, 1, "DAY", "current + 1");
        IndicatorRouter router = new IndicatorRouter(List.of(def));
        OrderFinalState match = new OrderFinalState("o1", "EVT_PAY", 1L, Map.of("merchantId", "M1"));
        OrderFinalState miss = new OrderFinalState("o2", "OTHER", 1L, Map.of("merchantId", "M1"));
        assertThat(router.route(match)).containsExactly(def);
        assertThat(router.route(miss)).isEmpty();
    }

    @Test
    void dimensionKey_builtFromDimensions() {
        IndicatorDef def = IndicatorDef.forTest("txn_cnt", List.of("merchant"), 3600, 24, "current + 1");
        OrderFinalState order = new OrderFinalState("o1", "E", 1L, Map.of("merchant", "M1"));
        assertThat(IndicatorRouter.dimensionKey(def, order)).isEqualTo("merchant#M1;");
    }

    // —— 累计脚本（R8.2/R8.5） ——

    @Test
    void accumulate_countScript() {
        AccumulationScriptEvaluator eval = new AccumulationScriptEvaluator();
        OrderFinalState order = new OrderFinalState("o1", "E", 1L, Map.of("amount", 100));
        assertThat(eval.accumulate("current + 1", 4.0, order)).isEqualTo(5.0);
    }

    @Test
    void accumulate_sumScript_usesOrderField() {
        AccumulationScriptEvaluator eval = new AccumulationScriptEvaluator();
        OrderFinalState order = new OrderFinalState("o1", "E", 1L, Map.of("amount", 100));
        assertThat(eval.accumulate("current + amount", 50.0, order)).isEqualTo(150.0);
    }

    @Test
    void accumulate_scriptException_propagates() {
        AccumulationScriptEvaluator eval = new AccumulationScriptEvaluator();
        OrderFinalState order = new OrderFinalState("o1", "E", 1L, Map.of());
        // 引用不存在的变量将导致执行异常（由算子按 R8.5 处理）
        assertThatThrownBy(() -> eval.accumulate("current + missingVar", 1.0, order))
                .isInstanceOf(Exception.class);
    }
}
