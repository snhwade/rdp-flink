package com.riskplatform.flink.operator;

import com.riskplatform.common.model.IndicatorSliceUpdate;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 按 (refName, dimensionKey) 分区：幂等去重 + 累计脚本求增量 → 下发 Kafka 事件。
 *
 * <p>存储写入由下游多路 Kafka 消费者承担（Redis / ES 等）。
 */
public class IndicatorAccumulateEmitFunction
        extends KeyedProcessFunction<String, RoutedOrder, IndicatorSliceUpdate> {

    private transient AccumulationScriptEvaluator evaluator;
    private transient MapState<String, Boolean> processedOrders;

    @Override
    public void open(Configuration parameters) {
        evaluator = new AccumulationScriptEvaluator();
        processedOrders = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("processedOrders", String.class, Boolean.class));
    }

    @Override
    public void processElement(RoutedOrder routed, Context ctx, Collector<IndicatorSliceUpdate> out)
            throws Exception {
        String orderId = routed.order().getOrderId();
        if (Boolean.TRUE.equals(processedOrders.get(orderId))) {
            return;
        }

        long sliceSeconds = routed.def().sliceSeconds();
        long windowSeconds = sliceSeconds * Math.max(1, routed.def().windowSlices());
        long ttlSeconds = windowSeconds + sliceSeconds;

        long eventSec = routed.order().getEventEpochMs() / 1000L;
        long sliceTs = (eventSec / sliceSeconds) * sliceSeconds;
        String granularity = routed.def().sliceGranularity();
        String sliceKey = "ind:" + routed.refName() + ":" + routed.dimensionKey()
                + ":" + granularity + ":" + sliceTs;

        double increment;
        try {
            increment = evaluator.accumulate(routed.def().accScript(), 0.0d, routed.order());
        } catch (Exception e) {
            System.err.println("[IndicatorAccumulateEmit] 累计脚本异常，跳过订单 "
                    + orderId + " 指标 " + routed.refName() + "，原因: " + e.getMessage());
            return;
        }

        processedOrders.put(orderId, Boolean.TRUE);
        out.collect(new IndicatorSliceUpdate(
                routed.refName(),
                routed.dimensionKey(),
                granularity,
                sliceTs,
                increment,
                orderId,
                sliceKey,
                ttlSeconds));
    }
}
