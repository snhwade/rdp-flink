# indicator-accumulation

Flink 指标流累计模块：消费订单终态，计算切片增量，回写 Kafka。

详细架构说明见 monorepo 文档：[docs/architecture-indicator-pipeline.md](../docs/architecture-indicator-pipeline.md)

## 拓扑（当前实现）

```
order-final-state (Kafka)
  → 反序列化 + 动态指标路由（rule-config 周期同步）
  → keyBy(refName | dimensionKey)
  → IndicatorAccumulateEmitFunction（幂等 + Aviator 增量）
  → indicator-slice-updates (Kafka)
```

下游由 **indicator-store-service**（或其他消费者）写入 Redis / ES。

## 构建与运行

```powershell
mvn clean package -DskipTests
java -jar target/indicator-accumulation-1.0.0-SNAPSHOT.jar `
  --kafka localhost:9092 `
  --sink-topic indicator-slice-updates `
  --rule-config http://localhost:8082
```

## 主类

`com.riskplatform.flink.IndicatorAccumulationJob`
