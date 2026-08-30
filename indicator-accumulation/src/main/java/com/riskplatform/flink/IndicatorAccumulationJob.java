package com.riskplatform.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskplatform.common.model.IndicatorSliceUpdate;
import com.riskplatform.flink.config.DynamicIndicatorRegistry;
import com.riskplatform.flink.config.JobConfig;
import com.riskplatform.flink.model.IndicatorDef;
import com.riskplatform.flink.operator.DeserializeResult;
import com.riskplatform.flink.operator.IndicatorAccumulateEmitFunction;
import com.riskplatform.flink.operator.IndicatorRouter;
import com.riskplatform.flink.operator.IndicatorSliceUpdateKafkaSerializer;
import com.riskplatform.flink.operator.OrderDeserializer;
import com.riskplatform.flink.operator.RoutedOrder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 指标累计 Flink 作业入口（R8）。
 *
 * <p>拓扑：Kafka Source（订单终态）→ 反序列化 + 动态指标路由 → 累计增量计算
 * → Kafka Sink（指标切片增量事件）。下游多路消费者可按配置写入 Redis / ES 等存储。
 */
public class IndicatorAccumulationJob {

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpointing(env, config);
        buildTopology(env, config);
        env.execute("indicator-accumulation-job");
    }

    static void configureCheckpointing(StreamExecutionEnvironment env, JobConfig config) {
        env.enableCheckpointing(config.getCheckpointIntervalMs());
    }

    static SingleOutputStreamOperator<IndicatorSliceUpdate> buildTopology(
            StreamExecutionEnvironment env, JobConfig config) {

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getSourceTopic())
                .setGroupId(config.getConsumerGroup())
                .setStartingOffsets("latest".equalsIgnoreCase(config.getStartingOffsets())
                        ? OffsetsInitializer.latest()
                        : OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> raw = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "order-final-state-source");
        raw.setParallelism(config.getSourceParallelism());

        DataStream<RoutedOrder> routed = raw
                .process(new DeserializeRouteProcessFunction(config))
                .name("deserialize-route");

        SingleOutputStreamOperator<IndicatorSliceUpdate> updates = routed
                .keyBy(r -> r.refName() + "|" + r.dimensionKey())
                .process(new IndicatorAccumulateEmitFunction())
                .name("accumulate-emit");

        KafkaSink<IndicatorSliceUpdate> sink = KafkaSink.<IndicatorSliceUpdate>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setRecordSerializer(new IndicatorSliceUpdateKafkaSerializer(config.getSinkTopic()))
                .build();

        updates.sinkTo(sink).name("indicator-slice-kafka-sink");

        return updates;
    }

    /**
     * 反序列化 + 指标路由（指标定义从 rule-config 动态刷新，与管理页同步）。
     */
    static final class DeserializeRouteProcessFunction extends ProcessFunction<String, RoutedOrder> {

        private final JobConfig config;
        private transient DynamicIndicatorRegistry registry;
        private transient OrderDeserializer deserializer;

        DeserializeRouteProcessFunction(JobConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) {
            registry = new DynamicIndicatorRegistry(
                    config.getRuleConfigBaseUrl(), config.getDefinitionsRefreshMs());
            registry.open();
            deserializer = new OrderDeserializer(new ObjectMapper());
        }

        @Override
        public void processElement(String json, Context ctx, Collector<RoutedOrder> out) {
            DeserializeResult result = deserializer.deserialize(json);
            if (!result.success()) {
                System.err.println("[deserialize] 丢弃消息: " + result.failReason());
                return;
            }
            List<IndicatorDef> definitions = registry.current();
            IndicatorRouter router = new IndicatorRouter(definitions);
            for (IndicatorDef def : router.route(result.order())) {
                String dimKey = IndicatorRouter.cleanDimensionKey(def, result.order());
                out.collect(new RoutedOrder(def.refName(), dimKey, def, result.order()));
            }
        }
    }
}
