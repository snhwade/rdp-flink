package com.riskplatform.flink.operator;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 按 (refName, dimensionKey) 分区的有状态切片累计算子（R8.3 幂等 / R8.5 累计脚本 / R8.6）。
 *
 * <p>状态：
 * <ul>
 *   <li>{@code sliceValues}（MapState：sliceTs→当前值）：各切片累计值；</li>
 *   <li>{@code processedOrders}（MapState：orderId→1）：幂等去重，重复订单不再累计（R8.6）。</li>
 * </ul>
 *
 * <p>每条 {@link RoutedOrder} 计算其事件时间所属切片，执行累计脚本得到新值，更新状态并下发
 * {@code (sliceKey, newValue)} 供 Redis Sink 写入。脚本异常则跳过该消息并记录（R8.5）。
 */
public class SliceAccumulateFunction
        extends KeyedProcessFunction<String, RoutedOrder, Tuple2<String, Double>> {

    private transient AccumulationScriptEvaluator evaluator;

    private transient MapState<Long, Double> sliceValues;
    private transient MapState<String, Boolean> processedOrders;
    private transient ValueState<String> refNameState;

    @Override
    public void open(Configuration parameters) {
        evaluator = new AccumulationScriptEvaluator();
        sliceValues = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("sliceValues", Long.class, Double.class));
        processedOrders = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("processedOrders", String.class, Boolean.class));
        refNameState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("refName",
                        TypeInformation.of(new TypeHint<String>() {})));
    }

    @Override
    public void processElement(RoutedOrder routed, Context ctx,
                               Collector<Tuple2<String, Double>> out) throws Exception {
        String orderId = routed.order().getOrderId();
        // R8.6 幂等：同一 orderId 在该分区已处理则跳过
        if (Boolean.TRUE.equals(processedOrders.get(orderId))) {
            return;
        }

        long sliceSeconds = routed.def().sliceSeconds();
        long eventSec = routed.order().getEventEpochMs() / 1000L;
        long sliceTs = (eventSec / sliceSeconds) * sliceSeconds;

        Double current = sliceValues.get(sliceTs);
        double base = current == null ? 0d : current;

        double newValue;
        try {
            newValue = evaluator.accumulate(routed.def().accScript(), base, routed.order());
        } catch (Exception e) {
            // R8.5：脚本异常跳过该消息并记录，不影响其它消息
            System.err.println("[SliceAccumulate] 累计脚本异常，跳过订单 " + orderId
                    + " 指标 " + routed.refName() + "，原因: " + e.getMessage());
            return;
        }

        sliceValues.put(sliceTs, newValue);
        processedOrders.put(orderId, Boolean.TRUE);
        refNameState.update(routed.refName());

        String granularity = granularityName(sliceSeconds);
        String sliceKey = "ind:" + routed.refName() + ":" + routed.dimensionKey()
                + ":" + granularity + ":" + sliceTs;
        out.collect(Tuple2.of(sliceKey, newValue));
    }

    /** 切片宽度（秒）映射到 indicator-store 的粒度枚举名。 */
    static String granularityName(long sliceSeconds) {
        if (sliceSeconds == 60L) {
            return "MINUTE";
        }
        if (sliceSeconds == 3600L) {
            return "HOUR";
        }
        return "DAY";
    }
}
