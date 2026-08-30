package com.riskplatform.flink.config;

import java.io.Serializable;

/**
 * Flink 指标累计作业配置（R8.1）。
 *
 * <p>从作业参数/环境变量读取，提供默认值以便本地与测试运行。
 */
public class JobConfig implements Serializable {

    private final String kafkaBootstrapServers;
    private final String sourceTopic;
    private final String sinkTopic;
    private final String consumerGroup;
    private final String dlqTopic;
    private final long checkpointIntervalMs;
    private final int sourceParallelism;
    /** Kafka 起始位点：earliest | latest */
    private final String startingOffsets;
    /** rule-config 基址（拉取 ONLINE 指标定义） */
    private final String ruleConfigBaseUrl;
    /** 指标定义刷新周期（毫秒，与 indicator-store 默认 30s 一致） */
    private final long definitionsRefreshMs;

    public JobConfig(String kafkaBootstrapServers, String sourceTopic, String sinkTopic,
                     String consumerGroup, String dlqTopic, long checkpointIntervalMs,
                     int sourceParallelism, String startingOffsets,
                     String ruleConfigBaseUrl, long definitionsRefreshMs) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.sourceTopic = sourceTopic;
        this.sinkTopic = sinkTopic;
        this.consumerGroup = consumerGroup;
        this.dlqTopic = dlqTopic;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.sourceParallelism = sourceParallelism;
        this.startingOffsets = startingOffsets;
        this.ruleConfigBaseUrl = ruleConfigBaseUrl;
        this.definitionsRefreshMs = definitionsRefreshMs;
    }

    /** 默认配置（本地/测试）。支持环境变量覆盖 Kafka 地址。 */
    public static JobConfig defaults() {
        return new JobConfig(
                env("KAFKA_BOOTSTRAP", "localhost:9092"),
                "order-final-state",
                env("INDICATOR_SLICE_TOPIC", "indicator-slice-updates"),
                "indicator-accumulation",
                "order-final-state-dlq",
                60_000L,
                1,
                "earliest",
                env("RULE_CONFIG_URL", "http://localhost:8082"),
                Long.parseLong(env("INDICATOR_DEFINITIONS_REFRESH_MS", "30000")));
    }

    /**
     * 从作业程序参数构建配置（Flink 集群提交场景）。
     *
     * <p>参数在 TaskManager 上随作业分发生效。支持：
     * {@code --kafka} {@code --sink-topic} {@code --group} {@code --offsets} 等。
     */
    public static JobConfig fromArgs(String[] args) {
        JobConfig base = defaults();
        String kafka = base.kafkaBootstrapServers;
        String sinkTopic = base.sinkTopic;
        String group = base.consumerGroup;
        String offsets = base.startingOffsets;
        String ruleConfig = base.ruleConfigBaseUrl;
        long refreshMs = base.definitionsRefreshMs;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--kafka" -> kafka = args[i + 1];
                case "--sink-topic" -> sinkTopic = args[i + 1];
                case "--group" -> group = args[i + 1];
                case "--offsets" -> offsets = args[i + 1];
                case "--rule-config" -> ruleConfig = args[i + 1];
                case "--definitions-refresh-ms" -> refreshMs = Long.parseLong(args[i + 1]);
                default -> { /* 忽略未知参数（含已废弃的 --redis-host/--redis-port） */ }
            }
        }
        return new JobConfig(kafka, base.sourceTopic, sinkTopic, group, base.dlqTopic,
                base.checkpointIntervalMs, base.sourceParallelism, offsets,
                ruleConfig, refreshMs);
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public String getSinkTopic() {
        return sinkTopic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getDlqTopic() {
        return dlqTopic;
    }

    public long getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public int getSourceParallelism() {
        return sourceParallelism;
    }

    /** earliest 或 latest（默认 earliest）。 */
    public String getStartingOffsets() {
        return startingOffsets;
    }

    public String getRuleConfigBaseUrl() {
        return ruleConfigBaseUrl;
    }

    public long getDefinitionsRefreshMs() {
        return definitionsRefreshMs;
    }
}
