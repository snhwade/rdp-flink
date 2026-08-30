package com.riskplatform.flink.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskplatform.flink.model.OrderFinalState;

/**
 * 订单终态数据反序列化（R8.4）。
 *
 * <p>JSON → {@link OrderFinalState}；反序列化失败或缺少 orderId 返回失败结果（路由死信主题），
 * 不抛异常以保证流不中断。
 */
public class OrderDeserializer {

    private final ObjectMapper objectMapper;

    public OrderDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public DeserializeResult deserialize(String json) {
        if (json == null || json.isBlank()) {
            return DeserializeResult.fail(json, "空消息");
        }
        try {
            OrderFinalState order = objectMapper.readValue(json, OrderFinalState.class);
            if (order.getOrderId() == null || order.getOrderId().isBlank()) {
                return DeserializeResult.fail(json, "缺少 orderId");
            }
            return DeserializeResult.ok(order);
        } catch (Exception e) {
            return DeserializeResult.fail(json, "反序列化失败: " + e.getMessage());
        }
    }
}
