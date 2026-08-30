package com.riskplatform.flink.operator;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.riskplatform.flink.model.OrderFinalState;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 累计脚本求值（R8.2/R8.5）。
 *
 * <p>用 Aviator 执行指标定义的累计脚本，输入为切片当前值 {@code current} 与订单字段，
 * 输出为新的切片累计值。脚本异常向上抛出，由作业算子按 R8.5 跳过+告警处理。
 */
public class AccumulationScriptEvaluator {

    private final AviatorEvaluatorInstance instance = AviatorEvaluator.newInstance();
    private final Map<String, Expression> cache = new ConcurrentHashMap<>();

    /**
     * 执行累计脚本。
     *
     * @param accScript   Aviator 表达式（如 "current + 1" 或 "current + amount"）
     * @param current     切片当前值
     * @param order       订单（其 fields 作为可引用变量）
     * @return 新的切片累计值
     */
    public double accumulate(String accScript, double current, OrderFinalState order) {
        Map<String, Object> env = new HashMap<>();
        env.put("current", current);
        if (order.getFields() != null) {
            env.putAll(order.getFields());
        }
        Expression expression = cache.computeIfAbsent(accScript, s -> instance.compile(s, true));
        Object result = expression.execute(env);
        if (result instanceof Number n) {
            return n.doubleValue();
        }
        throw new IllegalStateException("累计脚本未返回数值: " + result);
    }
}
