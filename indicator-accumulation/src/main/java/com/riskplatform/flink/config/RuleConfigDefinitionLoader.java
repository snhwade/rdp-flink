package com.riskplatform.flink.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskplatform.flink.model.IndicatorDef;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 从 rule-config 拉取上线指标定义（与 indicator-store {@code IndicatorDefinitionProvider} 同源）。
 */
public class RuleConfigDefinitionLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> LIST_MAP =
            new TypeReference<>() {
            };

    private final String ruleConfigBaseUrl;
    private final HttpClient httpClient;

    public RuleConfigDefinitionLoader(String ruleConfigBaseUrl) {
        this.ruleConfigBaseUrl = trimTrailingSlash(ruleConfigBaseUrl);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /** GET /api/v1/indicator-definitions?status=ONLINE */
    public List<IndicatorDef> fetchOnlineDefinitions() throws Exception {
        URI uri = URI.create(ruleConfigBaseUrl + "/api/v1/indicator-definitions?status=ONLINE");
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(15))
                .GET()
                .header("Accept", "application/json")
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("rule-config HTTP " + response.statusCode() + ": " + response.body());
        }
        List<Map<String, Object>> list = MAPPER.readValue(response.body(), LIST_MAP);
        if (list == null || list.isEmpty()) {
            return List.of();
        }
        List<IndicatorDef> defs = new ArrayList<>(list.size());
        for (Map<String, Object> row : list) {
            defs.add(fromApiRow(row));
        }
        return defs;
    }

    @SuppressWarnings("unchecked")
    static IndicatorDef fromApiRow(Map<String, Object> row) {
        String refName = String.valueOf(row.get("refName"));
        List<String> eventTypeCodes = (List<String>) row.getOrDefault("eventTypeCodes", List.of());
        List<String> dimensions = (List<String>) row.getOrDefault("dimensions", List.of());
        int windowDays = row.get("windowDays") == null ? 1 : ((Number) row.get("windowDays")).intValue();
        String sliceGranularity = String.valueOf(row.getOrDefault("sliceGranularity", "DAY"));
        String accScript = String.valueOf(row.getOrDefault("accScript", "current + 1"));
        long sliceSeconds = SliceGranularityNames.stepSeconds(sliceGranularity);
        int windowSlices = SliceGranularityNames.windowSlices(windowDays, sliceSeconds);
        return new IndicatorDef(
                refName,
                eventTypeCodes,
                dimensions,
                sliceSeconds,
                windowSlices,
                sliceGranularity.trim().toUpperCase(),
                accScript);
    }

    private static String trimTrailingSlash(String url) {
        if (url == null || url.isBlank()) {
            return "http://localhost:8082";
        }
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
