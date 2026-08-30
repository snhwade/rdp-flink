package com.riskplatform.flink.accumulate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 指标累计核心逻辑（R8.6 幂等、R8.7 窗口老化）。
 *
 * <p>该类抽离 Flink 算子中的纯累计逻辑以便属性测试：
 * <ul>
 *   <li>按 orderId 幂等去重：同一订单重复应用不改变累计结果（P4）；</li>
 *   <li>按切片（sliceTs）累加；窗口当前值仅由 [now-window, now] 内切片求和（P5）。</li>
 * </ul>
 *
 * <p>累计采用计数/求和语义（每条订单对其所属切片贡献一个增量值，默认 1）。
 */
public class IndicatorAccumulator {

    private final long stepSeconds;
    private final int windowSlices;

    /** 每个切片的累计值。key=sliceTs（切片起点秒），value=累计值。 */
    private final Map<Long, Double> sliceValues = new HashMap<>();
    /** 已处理订单（幂等去重）。 */
    private final Set<String> processedOrders = new HashSet<>();

    /**
     * @param stepSeconds  切片宽度（秒）
     * @param windowSlices 窗口包含的切片数（windowSeconds / stepSeconds）
     */
    public IndicatorAccumulator(long stepSeconds, int windowSlices) {
        this.stepSeconds = stepSeconds;
        this.windowSlices = windowSlices;
    }

    /**
     * 应用一条订单终态数据。
     *
     * @param orderId      订单唯一标识（幂等键）
     * @param eventEpochSec 事件时间（秒）
     * @param increment    增量值（计数场景为 1）
     * @return 是否实际累计（false 表示重复订单被跳过）
     */
    public boolean apply(String orderId, long eventEpochSec, double increment) {
        if (!processedOrders.add(orderId)) {
            return false; // R8.6 幂等：重复订单跳过
        }
        long sliceTs = (eventEpochSec / stepSeconds) * stepSeconds;
        sliceValues.merge(sliceTs, increment, Double::sum);
        return true;
    }

    /**
     * 计算窗口当前值：仅累加 [nowSliceTs - (windowSlices-1)*step, nowSliceTs] 范围内的切片（R8.7）。
     *
     * @param nowEpochSec 当前时刻（秒）
     */
    public double currentValue(long nowEpochSec) {
        long nowSlice = (nowEpochSec / stepSeconds) * stepSeconds;
        long earliest = nowSlice - (long) (windowSlices - 1) * stepSeconds;
        double sum = 0d;
        for (Map.Entry<Long, Double> e : sliceValues.entrySet()) {
            if (e.getKey() >= earliest && e.getKey() <= nowSlice) {
                sum += e.getValue();
            }
        }
        return sum;
    }
}
