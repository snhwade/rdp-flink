package com.riskplatform.flink.operator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskplatform.common.model.IndicatorSliceUpdate;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * 将 {@link IndicatorSliceUpdate} 序列化为 Kafka JSON 记录，分区键为 refName|dimensionKey。
 */
public class IndicatorSliceUpdateKafkaSerializer
        implements KafkaRecordSerializationSchema<IndicatorSliceUpdate> {

    private final String topic;
    private transient ObjectMapper mapper;

    public IndicatorSliceUpdateKafkaSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            IndicatorSliceUpdate element,
            KafkaRecordSerializationSchema.KafkaSinkContext context,
            Long timestamp) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            byte[] value = mapper.writeValueAsBytes(element);
            String partitionKey = element.refName() + "|" + element.dimensionKey();
            byte[] key = partitionKey.getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topic, key, value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("序列化 IndicatorSliceUpdate 失败", e);
        }
    }
}
