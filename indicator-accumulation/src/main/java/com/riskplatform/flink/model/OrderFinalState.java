package com.riskplatform.flink.model;

import java.io.Serializable;
import java.util.Map;

/**
 * 订单终态数据（业务方推送至 Kafka 的消息载体，R8.1/R8.2）。
 *
 * <p>{@code orderId} 用于幂等去重（R8.6）；{@code fields} 承载累计脚本所需的业务字段，
 * 由指标定义的统计维度从中提取。
 */
public class OrderFinalState implements Serializable {

    private String orderId;
    private String eventTypeCode;
    private long eventEpochMs;
    private Map<String, Object> fields;

    public OrderFinalState() {
    }

    public OrderFinalState(String orderId, String eventTypeCode, long eventEpochMs, Map<String, Object> fields) {
        this.orderId = orderId;
        this.eventTypeCode = eventTypeCode;
        this.eventEpochMs = eventEpochMs;
        this.fields = fields;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getEventTypeCode() {
        return eventTypeCode;
    }

    public void setEventTypeCode(String eventTypeCode) {
        this.eventTypeCode = eventTypeCode;
    }

    public long getEventEpochMs() {
        return eventEpochMs;
    }

    public void setEventEpochMs(long eventEpochMs) {
        this.eventEpochMs = eventEpochMs;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }
}
