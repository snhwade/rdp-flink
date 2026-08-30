package com.riskplatform.flink.operator;

import com.riskplatform.flink.model.IndicatorDef;
import com.riskplatform.flink.model.OrderFinalState;

import java.io.Serializable;

/**
 * 路由后的累计单元（R8.2/R8.3）：一条订单对一个适用指标定义的累计意图。
 *
 * <p>由 {@link IndicatorRouter} 将一条订单扇出为多个 RoutedOrder（每个适用指标定义一个），
 * 后续按 (refName, dimensionKey) keyBy 分区进行有状态累计。
 *
 * @param refName      指标引用名
 * @param dimensionKey 维度键（如 merchant#M001）
 * @param def          指标定义（含切片宽度/累计脚本）
 * @param order        原始订单（提供 orderId 幂等键、事件时间、字段）
 */
public record RoutedOrder(String refName, String dimensionKey, IndicatorDef def, OrderFinalState order)
        implements Serializable {
}
