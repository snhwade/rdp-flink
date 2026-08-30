package com.riskplatform.flink.operator;

import com.riskplatform.flink.model.OrderFinalState;

/**
 * 反序列化结果（R8.4）：成功携带订单，失败携带原始消息与原因（路由死信）。
 */
public record DeserializeResult(boolean success, OrderFinalState order, String rawMessage, String failReason) {

    public static DeserializeResult ok(OrderFinalState order) {
        return new DeserializeResult(true, order, null, null);
    }

    public static DeserializeResult fail(String rawMessage, String reason) {
        return new DeserializeResult(false, null, rawMessage, reason);
    }
}
