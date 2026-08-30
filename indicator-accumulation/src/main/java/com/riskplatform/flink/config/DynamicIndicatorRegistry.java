package com.riskplatform.flink.config;

import com.riskplatform.flink.model.IndicatorDef;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 指标定义本地快照：启动加载 + 周期刷新（与页面 rule-config 同步）。
 *
 * <p>拉取失败时保留上次成功快照；若从未成功则回退内置默认定义。
 */
public class DynamicIndicatorRegistry implements Serializable {

    private final String ruleConfigBaseUrl;
    private final long refreshMs;

    private transient AtomicReference<List<IndicatorDef>> snapshot;
    private transient volatile long lastRefreshAttemptMs;
    private transient RuleConfigDefinitionLoader loader;

    public DynamicIndicatorRegistry(String ruleConfigBaseUrl, long refreshMs) {
        this.ruleConfigBaseUrl = ruleConfigBaseUrl;
        this.refreshMs = Math.max(5_000L, refreshMs);
    }

    public void open() {
        this.snapshot = new AtomicReference<>(BootstrapIndicatorDefinitions.defaults());
        this.loader = new RuleConfigDefinitionLoader(ruleConfigBaseUrl);
        refreshNow();
    }

    /** 供算子每条消息前调用：到期则异步触发刷新（同步实现，轻量 HTTP）。 */
    public List<IndicatorDef> current() {
        long now = System.currentTimeMillis();
        if (now - lastRefreshAttemptMs >= refreshMs) {
            refreshNow();
        }
        return snapshot.get();
    }

    public synchronized void refreshNow() {
        lastRefreshAttemptMs = System.currentTimeMillis();
        try {
            List<IndicatorDef> fetched = loader.fetchOnlineDefinitions();
            if (!fetched.isEmpty()) {
                snapshot.set(fetched);
                System.out.println("[indicator-definitions] 已从 rule-config 刷新 "
                        + fetched.size() + " 个上线指标: "
                        + fetched.stream().map(IndicatorDef::refName).toList());
            } else if (snapshot.get().isEmpty()) {
                snapshot.set(BootstrapIndicatorDefinitions.defaults());
                System.err.println("[indicator-definitions] rule-config 无 ONLINE 指标，使用内置默认");
            }
        } catch (Exception ex) {
            if (snapshot.get().isEmpty()) {
                snapshot.set(BootstrapIndicatorDefinitions.defaults());
            }
            System.err.println("[indicator-definitions] 拉取失败，保留快照 "
                    + snapshot.get().size() + " 个，原因: " + ex.getMessage());
        }
    }
}
