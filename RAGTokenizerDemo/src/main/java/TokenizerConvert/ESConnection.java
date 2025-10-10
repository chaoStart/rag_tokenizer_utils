package TokenizerConvert;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.elasticsearch.core.search.SourceFilter;
import co.elastic.clients.elasticsearch.core.search.TrackHits;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.CredentialsProvider;
import org.elasticsearch.client.RestClientBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

public class ESConnection {
    private static final Logger logger = LoggerFactory.getLogger(ESConnection.class);
    private InfoResponse info;
    private String indexName;
    private ElasticsearchClient es;

    public ESConnection() {
        this.info = null;
        this.conn();
        this.indexName = "ragflow_sciyonff8bcdc11efbdcf88aedd524325";
    }

    private void conn() {
        int maxRetries = 10;
        for (int i = 0; i < maxRetries; i++) {
            try {
                // 创建认证凭据
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials("elastic", "sciyon")
                );

                // 构建 RestClient
                RestClientBuilder restClientBuilder = RestClient.builder(
                                new HttpHost("10.3.24.46", 9200, "http")
                        )
                        .setHttpClientConfigCallback(httpClientBuilder -> {
                            // 设置认证
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            // 可选：设置超时（单位：毫秒）
                            httpClientBuilder.setConnectionTimeToLive(600_000, TimeUnit.MILLISECONDS);
                            return httpClientBuilder;
                        })
                        .setRequestConfigCallback(requestConfigBuilder -> {
                            // 设置请求超时（连接、socket、请求）
                            return requestConfigBuilder
                                    .setConnectTimeout(600_000)
                                    .setSocketTimeout(600_000);
                        });

                RestClient restClient = restClientBuilder.build();

                // 创建传输层
                ElasticsearchTransport transport = new RestClientTransport(
                        restClient,
                        new JacksonJsonpMapper()
                );

                // 创建 ElasticsearchClient
                this.es = new ElasticsearchClient(transport);

                // 测试连接
                this.info = es.info();
                logger.info("Connected to Elasticsearch.");
                return; // 成功连接，退出循环

            } catch (Exception e) {
                logger.error("Failed to connect to Elasticsearch: " + e.getMessage(), e);
                try {
                    Thread.sleep(1000); // 等待1秒后重试
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        // 如果循环结束仍未连接成功，可抛出异常或处理失败逻辑
        if (this.es == null) {
            throw new RuntimeException("Unable to connect to Elasticsearch after " + maxRetries + " attempts.");
        }
    }

    public ElasticsearchClient getClient() {
        return es;
    }

    public InfoResponse getInfo() {
        return info;
    }

    public String getIdxnm() {
        return indexName;
    }

    public boolean ping() {
        try {
            return es.ping().value();
        } catch (IOException e) {
            return false;
        }
    }


    public long getTotal(SearchResponse<?> response) {
        if (response.hits().total() != null) {
            return response.hits().total().value();
        }
        return response.hits().hits().size();
    }

    public List<String> getDocIds(SearchResponse<Map> res) {
        List<String> ids = new ArrayList<>();

        if (res == null || res.hits() == null || res.hits().hits() == null) {
            return ids;
        }

        for (Hit<Map> hit : res.hits().hits()) {
            ids.add(hit.id());  // 等价于 Python 中的 d["_id"]
        }

        return ids;
    }

    public SearchResponse<Map> search(Map<String, Object> q, String idxnms, List<String> src, String timeout) throws Exception {
        String[] indices = (idxnms == null || idxnms.trim().isEmpty())
                ? new String[]{indexName}
                : idxnms.split(",");
        // 打印输出q查询语句包含的信息；
        System.out.println("**这里是组合了各种参数的查询条件**\n"+q);
 /*       Exception lastException = null;
        for (int i = 0; i < 3; i++) {
            try {
                Map<String, Object> queryMap = (Map<String, Object>) q.get("query");
                Query query = QueryConverter.parseQuery(queryMap);
                // 构建 SearchRequest
                SearchRequest.Builder builder = new SearchRequest.Builder()
                        .index(Arrays.asList("ragflow_sciyonff8bcdc11efbdcf88aedd524325"))
                        .timeout("300s")
                        .trackTotalHits(th -> th.enabled(true))
                        .source(s -> s.filter(f -> f.includes(src)))
                        .query(query);

                // 发起查询
                SearchResponse<Map> res = es.search(builder.build(), Map.class);

                if (Boolean.TRUE.equals(res.timedOut())) {
                    throw new Exception("Es Timeout.");
                }
                return res;

            } catch (Exception e) {
                lastException = e;
                logger.error("ES search exception: {} 【Q】: {}", e.getMessage(), q, e);
                if (e.getMessage() != null && e.getMessage().contains("Timeout")) {
                    continue;
                }
                throw e;
            }
        }

        logger.error("ES search timeout for 3 times!");
        throw lastException != null ? lastException : new Exception("ES search timeout.");
    }*/
        Exception lastException = null;
        ObjectMapper mapper = new ObjectMapper();

        for (int i = 0; i < 3; i++) {
            try {
                // 1. 解析 from 和 size
                Integer from = q.containsKey("from") ? ((Number) q.get("from")).intValue() : 0;
                Integer size = q.containsKey("size") ? ((Number) q.get("size")).intValue() : 10;

                // 2. 构建主 query
                Query mainQuery = buildQueryFromMap((Map<String, Object>) q.get("query"));

                // 3. 构建 KNN 查询（如果存在）
                KnnQuery knnQuery = null;
                if (q.containsKey("knn")) {
                    Map<String, Object> knnMap = (Map<String, Object>) q.get("knn");
                    List<Float> vector = (List<Float>) knnMap.get("query_vector");
                    String field = (String) knnMap.get("field");
                    int k = ((Number) knnMap.get("k")).intValue();
                    int numCandidates = ((Number) knnMap.get("num_candidates")).intValue();
                    float similarity = ((Number) knnMap.get("similarity")).floatValue();

                    // 构建 KNN filter
                    Query knnFilter;
                    if (knnMap.containsKey("filter")) {
                        knnFilter = buildQueryFromMap((Map<String, Object>) knnMap.get("filter"));
                    } else {
                        knnFilter = null;
                    }

                    knnQuery = KnnQuery.of(kb -> kb
                            .field(field)
                            .k(k)
                            .numCandidates(numCandidates)
                            .similarity(similarity)
                            .queryVector(vector)
                            .filter(knnFilter)
                    );
                }

                // 4. 构建 SearchRequest
                SearchRequest.Builder builder = new SearchRequest.Builder()
                        .index(Arrays.asList(indices))
                        .from(from)
                        .size(size)
                        .timeout("30s")
                        .trackTotalHits(th -> th.enabled(true))
                        .source(s -> s.filter(f -> f.includes(src)))
                        .query(mainQuery);

                // 5. 添加 KNN 查询（ES 8.11 支持 top-level knn）
                if (knnQuery != null) {
                    builder.knn(knnQuery);
                }

                // 6. 执行查询
                SearchResponse<Map> res = es.search(builder.build(), Map.class);

                if (Boolean.TRUE.equals(res.timedOut())) {
                    throw new Exception("Es Timeout.");
                }
                return res;

            } catch (Exception e) {
                lastException = e;
                logger.error("ES search exception: {} 【Q】: {}", e.getMessage(), q, e);
                if (e.getMessage() != null && e.getMessage().contains("Timeout")) {
                    continue;
                }
                throw e;
            }
        }

        logger.error("ES search timeout for 3 times!");
        throw lastException != null ? lastException : new Exception("ES search timeout.");
    }


    // 递归构建 Query 对象
    private Query buildQueryFromMap(Map<String, Object> queryMap) {
        if (queryMap == null || queryMap.isEmpty()) {
            return new Query.Builder().matchAll(m -> m).build();
        }

        String kind = (String) queryMap.get("_kind");
        Map<String, Object> value = (Map<String, Object>) queryMap.get("_value");

        if ("Bool".equals(kind)) {
            BoolQuery.Builder boolBuilder = new BoolQuery.Builder();

            // must
            if (value.containsKey("must")) {
                List<Map<String, Object>> mustList = (List<Map<String, Object>>) value.get("must");
                for (Map<String, Object> sub : mustList) {
                    boolBuilder.must(buildQueryFromMap(sub));
                }
            }

            // filter
            if (value.containsKey("filter")) {
                List<Map<String, Object>> filterList = (List<Map<String, Object>>) value.get("filter");
                for (Map<String, Object> sub : filterList) {
                    boolBuilder.filter(buildQueryFromMap(sub));
                }
            }

            // must_not
            if (value.containsKey("mustNot")) {
                List<Map<String, Object>> mustNotList = (List<Map<String, Object>>) value.get("mustNot");
                for (Map<String, Object> sub : mustNotList) {
                    boolBuilder.mustNot(buildQueryFromMap(sub));
                }
            }

            // should
            if (value.containsKey("should")) {
                List<Map<String, Object>> shouldList = (List<Map<String, Object>>) value.get("should");
                for (Map<String, Object> sub : shouldList) {
                    boolBuilder.should(buildQueryFromMap(sub));
                }
            }

            // boost
            if (value.containsKey("boost")) {
                float boost = ((Number) value.get("boost")).floatValue();
                boolBuilder.boost(boost);
            }

            return new Query.Builder().bool(boolBuilder.build()).build();
        }

        else if ("QueryString".equals(kind)) {
            Map<String, Object> v = value;
            List<String> fields = (List<String>) v.get("fields");
            String queryStr = (String) v.get("query");
            String typeStr = (String) v.get("type");
            String minimumShouldMatch = (String) v.get("minimumShouldMatch");
            float boost = ((Number) v.get("boost")).floatValue();

            QueryStringQuery.Builder qsBuilder = new QueryStringQuery.Builder()
                    .query(queryStr)
                    .fields(fields)
                    .minimumShouldMatch(minimumShouldMatch)
                    .boost(boost);

            if ("BestFields".equalsIgnoreCase(typeStr)) {
                qsBuilder.type(TextQueryType.BestFields);
            } else if ("MostFields".equalsIgnoreCase(typeStr)) {
                qsBuilder.type(TextQueryType.MostFields);
            } // 可扩展其他类型

            return new Query.Builder().queryString(qsBuilder.build()).build();
        }

        else if ("Terms".equals(kind)) {
            String field = (String) value.get("field");
            List<Map<String, Object>> termsList = (List<Map<String, Object>>) ((Map<String, Object>) value.get("terms")).get("_value");
            List<FieldValue> termValues = termsList.stream()
                    .map(termMap -> {
                        Object val = termMap.get("_value");
                        // 支持字符串或数字（根据实际数据类型）
                        if (val instanceof String) {
                            return FieldValue.of((String) val);
                        } else if (val instanceof Number) {
                            return FieldValue.of(String.valueOf((Number) val));
                        } else {
                            return FieldValue.of(val.toString()); // fallback
                        }
                    })
                    .collect(Collectors.toList());
            return new Query.Builder()
                    .terms(t -> t.field(field).terms(tu -> tu.value(termValues)))
                    .build();
        }

        else if ("Range".equals(kind)) {
            String field = (String) value.get("field");
            RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field(field);

            if (value.containsKey("lt")) {
                Number lt = (Number) ((Map<String, Object>) value.get("lt")).get("value");
                rangeBuilder.lt(JsonData.of(lt));
            }
            // 可扩展 gt, gte, lte 等

            return new Query.Builder().range(rangeBuilder.build()).build();
        }

        else {
            throw new IllegalArgumentException("Unsupported query kind: " + kind);
        }
    }
}

