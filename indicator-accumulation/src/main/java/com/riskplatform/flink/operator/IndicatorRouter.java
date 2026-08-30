package com.riskplatform.flink.operator;

import com.riskplatform.flink.model.IndicatorDef;
import com.riskplatform.flink.model.OrderFinalState;

import java.util.ArrayList;
import java.util.List;

/**
 * 指标路由（R8.2/R8.4）。
 *
 * <p>对一条订单，匹配"所有统计维度所需字段均存在于订单 fields"的指标定义；
 * 维度字段缺失的指标定义不参与累计（视为该订单对该指标不适用）。
 */
public class IndicatorRouter {

    private final List<IndicatorDef> definitions;

    public IndicatorRouter(List<IndicatorDef> definitions) {
        this.definitions = definitions;
    }

    /** 返回订单适用的指标定义（事件匹配 + 维度字段齐全）。 */
    public List<IndicatorDef> route(OrderFinalState order) {
        List<IndicatorDef> matched = new ArrayList<>();
        for (IndicatorDef def : definitions) {
            if (!def.matchesEvent(order.getEventTypeCode())) {
                continue;
            }
            if (hasAllDimensions(def, order)) {
                matched.add(def);
            }
        }
        return matched;
    }

    private boolean hasAllDimensions(IndicatorDef def, OrderFinalState order) {
        if (order.getFields() == null) {
            return def.dimensions().isEmpty();
        }
        for (String dim : def.dimensions()) {
            if (!order.getFields().containsKey(dim) || order.getFields().get(dim) == null) {
                return false;
            }
        }
        return true;
    }

    /** 维度键：按维度字段值拼接，作为 keyBy 分区键的一部分。 */
    public static String dimensionKey(IndicatorDef def, OrderFinalState order) {
        StringBuilder sb = new StringBuilder();
        for (String dim : def.dimensions()) {
            Object v = order.getFields() == null ? null : order.getFields().get(dim);
            sb.append(dim).append('#').append(v).append(';');
        }
        return sb.toString();
    }

    /**
     * 干净维度键，与 indicator-store 的累计/读取格式严格一致：
     * <ul>
     *   <li>单维度：直接取该维度值，如 {@code M001}（不加维度名前缀）；</li>
     *   <li>多维度：{@code dim1#v1;dim2#v2}（按维度顺序拼接，无结尾分隔符）。</li>
     * </ul>
     * 必须与 indicator-store 的 IndicatorAccumulateService.dimensionKey 保持一致，
     * 否则两方案写入的切片键不同、读路径无法读到 Flink 写入的指标。
     */
    public static String cleanDimensionKey(IndicatorDef def, OrderFinalState order) {
        List<String> dims = def.dimensions();
        if (dims.size() == 1) {
            Object v = order.getFields() == null ? null : order.getFields().get(dims.get(0));
            return String.valueOf(v);
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String dim : dims) {
            Object v = order.getFields() == null ? null : order.getFields().get(dim);
            if (!first) {
                sb.append(';');
            }
            sb.append(dim).append('#').append(v);
            first = false;
        }
        return sb.toString();
    }
}
