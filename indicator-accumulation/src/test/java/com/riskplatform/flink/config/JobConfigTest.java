package com.riskplatform.flink.config;

import com.riskplatform.flink.model.OrderFinalState;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Flink 作业配置与模型基础测试（R8.1）。
 * 拓扑/算子的端到端验证见任务 8.8（MiniCluster）。
 */
class JobConfigTest {

    @Test
    void defaults_areSane() {
        JobConfig c = JobConfig.defaults();
        assertEquals("order-final-state", c.getSourceTopic());
        assertEquals("indicator-slice-updates", c.getSinkTopic());
        assertEquals("indicator-accumulation", c.getConsumerGroup());
        assertEquals("order-final-state-dlq", c.getDlqTopic());
        assertEquals(60_000L, c.getCheckpointIntervalMs());
        assertEquals(1, c.getSourceParallelism());
    }

    @Test
    void orderFinalState_holdsFields() {
        OrderFinalState s = new OrderFinalState("o1", "B2B_RECV", 1_700_000_000_000L,
                Map.of("amount", 100));
        assertEquals("o1", s.getOrderId());
        assertEquals("B2B_RECV", s.getEventTypeCode());
        assertNotNull(s.getFields());
        assertEquals(100, s.getFields().get("amount"));
    }
}
