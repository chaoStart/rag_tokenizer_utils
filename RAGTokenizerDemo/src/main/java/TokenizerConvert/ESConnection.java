package TokenizerConvert;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.elasticsearch.core.search.SourceFilter;
import co.elastic.clients.elasticsearch.core.search.TrackHits;
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
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch._types.Time;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.CredentialsProvider;
import org.elasticsearch.client.RestClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import co.elastic.clients.elasticsearch._types.Time;


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
        Exception lastException = null;
        for (int i = 0; i < 3; i++) {
            try {
//                SearchRequest request = new SearchRequest.Builder()
//                        .index(Arrays.asList(indices))
//                        .timeout(String.valueOf(Time.of(t -> t.time(timeout))))
//                        .trackTotalHits(TrackHits.of(th -> th.enabled(true)))
//                        .source(s -> s.filter(f -> f.includes(src)))
//                        .build();
//                // 注意：这里 q 还要转成 Query 并加进 request，这里先留空
//                SearchResponse<Map> res = es.search(request, Map.class);
                // Python dict 转成 Java Map qMap
//                Query query = QueryBuilderUtil.fromMap(q);
                // 1. 提取 query 部分
                Map<String, Object> queryMap = (Map<String, Object>) q.get("query");
                Query query = QueryConverter.parseQuery(queryMap);
                // 2.构建 SearchRequest
                SearchRequest.Builder builder = new SearchRequest.Builder()
                        .index(Arrays.asList("ragflow_sciyonff8bcdc11efbdcf88aedd524325"))
                        .timeout("30s")
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
    }


}

