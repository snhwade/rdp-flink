package com.riskplatform.flink.accumulate;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.Size;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 指标累计幂等与窗口老化属性测试。
 *
 * <p>Feature: risk-decision-platform, Property 4: 指标累计幂等（Validates: Requirements 8.6）
 * <br>Feature: risk-decision-platform, Property 5: 窗口老化（Validates: Requirements 8.7）
 */
class IndicatorAccumulatorPropertyTest {

    private static final long STEP = 3600L;      // 小时切片
    private static final int WINDOW_SLICES = 24; // 1 天窗口
    private static final long NOW = 1_700_000_000L;

    record Order(String orderId, long eventSec, double inc) {
    }

    /**
     * Property 4: 对任意订单序列，将任意子集重复投递任意次，
     * 最终窗口值与"每个 orderId 恰好处理一次"一致。
     */
    @Property(tries = 200)
    void idempotent_underDuplicateDelivery(@ForAll("orders") @Size(min = 1, max = 30) List<Order> orders) {
        // 基准：每个订单处理一次
        IndicatorAccumulator baseline = new IndicatorAccumulator(STEP, WINDOW_SLICES);
        for (Order o : orders) {
            baseline.apply(o.orderId(), o.eventSec(), o.inc());
        }
        double expected = baseline.currentValue(NOW);

        // 重复投递：把订单列表复制多份并打乱
        List<Order> duplicated = new ArrayList<>(orders);
        duplicated.addAll(orders);
        duplicated.addAll(orders);
        Collections.shuffle(duplicated);

        IndicatorAccumulator withDup = new IndicatorAccumulator(STEP, WINDOW_SLICES);
        for (Order o : duplicated) {
            withDup.apply(o.orderId(), o.eventSec(), o.inc());
        }
        assertThat(withDup.currentValue(NOW)).isCloseTo(expected, org.assertj.core.api.Assertions.offset(1e-6));
    }

    /**
     * Property 5: 超出窗口的历史切片不参与当前值计算。
     */
    @Property(tries = 200)
    void windowAging_excludesOutOfWindowSlices(@ForAll("orders") @Size(min = 1, max = 30) List<Order> orders) {
        IndicatorAccumulator acc = new IndicatorAccumulator(STEP, WINDOW_SLICES);
        long windowSeconds = (long) WINDOW_SLICES * STEP;
        long earliest = ((NOW / STEP) * STEP) - (windowSeconds - STEP);

        double expectedInWindow = 0d;
        for (Order o : orders) {
            acc.apply(o.orderId(), o.eventSec(), o.inc());
            long sliceTs = (o.eventSec() / STEP) * STEP;
            if (sliceTs >= earliest && sliceTs <= (NOW / STEP) * STEP) {
                expectedInWindow += o.inc();
            }
        }
        assertThat(acc.currentValue(NOW)).isCloseTo(expectedInWindow, org.assertj.core.api.Assertions.offset(1e-6));
    }

    @Provide
    Arbitrary<List<Order>> orders() {
        // 唯一 orderId
        Arbitrary<String> ids = Arbitraries.strings().withCharRange('a', 'z').ofMinLength(3).ofMaxLength(8);
        // 事件时间覆盖 [NOW - 3 天, NOW]，确保部分落在窗口外
        Arbitrary<Long> times = Arbitraries.longs().between(NOW - 3 * 86400L, NOW);
        Arbitrary<Double> incs = Arbitraries.doubles().between(1.0, 5.0);
        Arbitrary<Order> one = Combinators.combine(ids, times, incs).as(Order::new);
        // 用 uniqueElements 保证 orderId 不重复（按 orderId 去重语义清晰）
        return one.list().ofMinSize(1).ofMaxSize(30).uniqueElements(Order::orderId);
    }
}
