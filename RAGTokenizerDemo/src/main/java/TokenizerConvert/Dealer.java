package TokenizerConvert;

import co.elastic.clients.elasticsearch._types.KnnQuery;
import co.elastic.clients.elasticsearch.core.search.BoundaryScanner;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import co.elastic.clients.elasticsearch.core.search.Highlight;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import java.nio.charset.StandardCharsets;

import java.util.*;
import java.util.regex.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Dealer {

    private RagTokenizer tokenizer=new RagTokenizer();
    private final EsQueryer qryr;
    private final ESConnection es; // 你现有的 ES 客户端
    private static final String SEARCH_ES_INDEX = "ragflow_sciyonff8bcdc11efbdcf88aedd524325";
    private static final Logger log = LoggerFactory.getLogger(Dealer.class);  // 在类中定义 logger
    private static final String BASE_URL = "http://10.3.24.46:9997/v1/embeddings";
    private static final String API_KEY = "123"; // 你的 key
    private final Gson gson = new Gson();

    public Dealer(ESConnection es) {
        this.es = es;
        this.qryr = new EsQueryer(es);
        this.qryr.flds =Arrays.asList(
                "title_tks^10",
                "title_sm_tks^5",
                "important_kwd^30",
                "important_tks^20",
                "content_ltks^2",
                "content_sm_ltks"
        );
    }

    public static String rmSpace(String txt) {
        txt = txt.replaceAll("([^a-z0-9.,\\)>]) +([^ ])", "$1$2");
        txt = txt.replaceAll("([^ ]) +([^a-z0-9.,\\(<])", "$1$2");
        return txt;
    }

    public static boolean isEnglish(List<String> texts) {
        if (texts == null || texts.isEmpty()) return false;
        int eng = 0;
        Pattern pattern = Pattern.compile("[ `a-zA-Z.,':;/\"?<>!()\\-]");
        for (String t : texts) {
            if (pattern.matcher(t.trim()).matches()) eng++;
        }
        return (double) eng / texts.size() > 0.8;
    }

    public static class Pair<L, R> {
        private final L left;
        private final R right;

        public Pair(L left, R right) {
            this.left = left;
            this.right = right;
        }

        public L getLeft() { return left; }
        public R getRight() { return right; }
    }

    private List<Pair<String, Long>> getAggregation(SearchResponse<Map> res, String g) {
        if (res.aggregations() == null || !res.aggregations().containsKey("aggs_" + g)) {
            return Collections.emptyList();
        }
        return Collections.emptyList();
    }

    public Map<String, Map<String, String>> getFields(SearchResponse<Map> sres, List<String> flds) {
        Map<String, Map<String, String>> res = new HashMap<>();

        // 如果字段列表为空，直接返回
        if (flds == null || flds.isEmpty()) {
            return res;
        }

        // 遍历 hits
        if (sres.hits() == null || sres.hits().hits().isEmpty()) {
            return res;
        }

        for (Hit<Map> hit : sres.hits().hits()) {
            Map<String, Object> source = hit.source();
            if (source == null) continue;

            // 提取指定字段
            Map<String, String> m = new HashMap<>();
            for (String n : flds) {
                Object v = source.get(n);
                if (v == null) continue;

                // 处理字段值
                if (v instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> listVal = (List<Object>) v;
                    List<String> strList = new ArrayList<>();
                    for (Object vv : listVal) {
                        if (vv instanceof List) {
                            List<?> subList = (List<?>) vv;
                            List<String> subStrList = new ArrayList<>();
                            for (Object vvv : subList) {
                                subStrList.add(String.valueOf(vvv));
                            }
                            strList.add(String.join("\t", subStrList));
                        } else {
                            strList.add(String.valueOf(vv));
                        }
                    }
                    m.put(n, String.join("\t", strList));
                } else {
                    // 如果不是 String 类型，转为 String
                    if (!(v instanceof String)) {
                        m.put(n, String.valueOf(v));
                    } else {
                        m.put(n, (String) v);
                    }
                }
            }

            // 如果字段非空，则放入结果
            if (!m.isEmpty()) {
                res.put(hit.id(), m);
            }
        }

        return res;
    }

    public Map<String, Object> getHighlight(SearchResponse<Map> res, List<String> keywords, String fieldnm) {
        Map<String, Object> ans = new HashMap<>();

        if (res == null || res.hits() == null || res.hits().hits() == null) {
            return ans;
        }

        for (Hit<Map> hit : res.hits().hits()) {
            Map<String, List<String>> hlts = hit.highlight();

            if (hlts == null || hlts.isEmpty()) {
                continue;
            }

            // 取第一个高亮字段的内容拼接
            String txt = String.join("...", hlts.values().iterator().next());

            // 如果不是英文，直接用高亮内容
            if (!isEnglish(Collections.singletonList(txt))) {
                ans.put(hit.id(), txt);
                continue;
            }

            // 否则用 source 字段内容
            Map<String, Object> source = hit.source();
            if (source == null || !source.containsKey(fieldnm)) {
                continue;
            }

            txt = String.valueOf(source.get(fieldnm));
            // 替换换行符
            txt = txt.replaceAll("[\r\n]", " ");

            List<String> txts = new ArrayList<>();

            // 按 .?!;\n 分句
            String[] sentences = txt.split("[.?!;\\n]");
            for (String t : sentences) {
                String processed = t;

                // 遍历关键词，高亮替换
                for (String w : keywords) {
                    String escaped = Pattern.quote(w);
                    String regex = "(^|[ .?/\\'\\\"\\(\\)!,:;-])(" + escaped + ")([ .?/\\'\\\"\\(\\)!,:;-])";
                    Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
                    Matcher matcher = pattern.matcher(processed);
                    processed = matcher.replaceAll("$1<em>$2</em>$3");
                }

                // 如果没有匹配到高亮，跳过
                if (!processed.matches(".*<em>[^<>]+</em>.*")) {
                    continue;
                }

                txts.add(processed);
            }

            if (!txts.isEmpty()) {
                ans.put(hit.id(), String.join("...", txts));
            } else {
                ans.put(hit.id(), String.join("...", hlts.values().iterator().next()));
            }
        }

        return ans;
    }

    public Map<String, Object> _vector(String text, double sim, int topk) throws Exception {
        // 1. 构造请求 JSON
        Map<String, Object> body = new HashMap<>();
        body.put("input", text);
        body.put("model", "bge-large-zh-v1.5");

        String jsonBody = gson.toJson(body);

        // 2. 发起 HTTP 请求
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost post = new HttpPost(BASE_URL);
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Authorization", "Bearer " + API_KEY);
        post.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));

        CloseableHttpResponse response = httpClient.execute(post);
        String responseStr = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

        response.close();
        httpClient.close();

        // 3. 解析 JSON 响应
        JsonObject json = JsonParser.parseString(responseStr).getAsJsonObject();
        JsonArray dataArray = json.getAsJsonArray("data");
        JsonObject firstObj = dataArray.get(0).getAsJsonObject();
        JsonArray embeddingArray = firstObj.getAsJsonArray("embedding");

        List<Float> qv = new ArrayList<>();
        for (JsonElement e : embeddingArray) {
            qv.add(e.getAsFloat());
        }

        // 4. 构造结果 map
        Map<String, Object> result = new HashMap<>();
        result.put("field", "q_" + qv.size() + "_vec");
        result.put("k", topk);
        result.put("similarity", sim);
        result.put("num_candidates", topk * 2);
        result.put("query_vector", qv);

        return result;
    }

    // 默认参数版本
    public Map<String, Object> _vector(String text) throws Exception {
        return _vector(text, 0.8, 10);
    }

    // 将文本转换为浮点向量
    public static List<Float> trans2floats(String txt) {
        if (StringUtils.isBlank(txt)) return new ArrayList<>();
        String[] arr = txt.split("\t");
        List<Float> floats = new ArrayList<>();
        for (String s : arr) {
            try {
                floats.add(Float.parseFloat(s));
            } catch (Exception e) {
                floats.add(0f);
            }
        }
        return floats;
    }

    // 给 BoolQuery 添加过滤条件
    public static Query addFilters(Query query, Map<String, Object> req) {
        BoolQuery.Builder boolBuilder;

        // 如果原始 query 已经是 bool，就在它的基础上加 filter
        if (query != null && query._kind() == Query.Kind.Bool) {
            boolBuilder = new BoolQuery.Builder().must(query.bool().must())
                    .should(query.bool().should())
                    .mustNot(query.bool().mustNot())
                    .filter(query.bool().filter());
        } else {
            // 否则新建一个 bool，把原始 query 放到 must 里
            boolBuilder = new BoolQuery.Builder();
            if (query != null) {
                boolBuilder.must(query);
            }
        }
        // kb_ids
        if (req.containsKey("kb_ids")) {
            @SuppressWarnings("unchecked")
            List<String> kbIds = (List<String>) req.get("kb_ids");
            if (kbIds != null && !kbIds.isEmpty()) {
                List<FieldValue> values = kbIds.stream()
                        .map(FieldValue::of)
                        .collect(Collectors.toList());
                boolBuilder.filter(f -> f.terms(t -> t.field("kb_id").terms(tt -> tt.value(values))));
            }
        }

        // doc_ids
        if (req.containsKey("doc_ids")) {
            @SuppressWarnings("unchecked")
            List<String> docIds = (List<String>) req.get("doc_ids");
            if (docIds != null && !docIds.isEmpty()) {
                List<FieldValue> values = docIds.stream()
                        .map(FieldValue::of)
                        .collect(Collectors.toList());
                boolBuilder.filter(f -> f.terms(t -> t.field("doc_id").terms(tt -> tt.value(values))));
            }
        }

        // docnm_kwd
        if (req.containsKey("docnm_kwd")) {
            @SuppressWarnings("unchecked")
            List<String> docnmKwd = (List<String>) req.get("docnm_kwd");
            if (docnmKwd != null && !docnmKwd.isEmpty()) {
                List<FieldValue> values = docnmKwd.stream()
                        .map(FieldValue::of)
                        .collect(Collectors.toList());
                boolBuilder.filter(f -> f.terms(t -> t.field("docnm_kwd").terms(tt -> tt.value(values))));
            }
        }

        // available_int
        if (req.containsKey("available_int")) {
            Object val = req.get("available_int");
            if (val instanceof Number && ((Number) val).intValue() == 0) {
                // available_int == 0 → range lt 1
                boolBuilder.filter(f -> f.range(r -> r.field("available_int").lt(JsonData.of(1))));
            } else {
                // available_int != 0 → must_not { range lt 1 }
                boolBuilder.filter(f -> f.bool(bb ->
                        bb.mustNot(mn -> mn.range(r -> r.field("available_int").lt(JsonData.of(1))))
                ));
            }
        }
        // 添加权重
        boolBuilder.boost(0.05f);
       return Query.of(q -> q.bool(boolBuilder.build()));
    }

    public SearchResult search(Map<String, Object> req, String indexName, boolean highlight) throws Exception {

        String question = req.get("question") != null ? req.get("question").toString() : "";
//        int pg = req.get("page") != null ? ((Number) req.get("page")).intValue() : 1;
        int pg = req.get("page") != null ? ((Number) req.get("page")).intValue() : 0;
        int ps = req.get("size") != null ? ((Number) req.get("size")).intValue() : 10;
        int topK = req.get("topk") != null ? ((Number) req.get("topk")).intValue() : 10;
        double similarity = req.get("similarity") != null ? ((Number) req.get("similarity")).doubleValue() : 0.8;

        // 解析返回字段
        @SuppressWarnings("unchecked")
        List<String> src = req.get("fields") != null
                ? (List<String>) req.get("fields")
                : Arrays.asList("docnm_kwd", "content_ltks", "kb_id", "title_tks", "important_kwd",
                "parent_id", "doc_id", "q_512_vec", "q_768_vec", "position_int", "knowledge_graph_kwd",
                "q_1024_vec", "q_1536_vec", "available_int", "content_with_weight", "faq_question");

        // 1. 获取bqry和keyword,然后用qPair把两个结果合并在一起返回成qPair
        EsQueryer.Pair<Query, List<String>> qPair = this.qryr.question(question, "qa", "30%");
        // 获取elasticsearch对象包装的带权重分词文本text和字段名称
        Query bqry = qPair.getbqry();
        // 获取ragflow中weight方法提取的关键词
        List<String> keywords = qPair.getkeywords();

        // 2. 添加过滤器；设置每一个查询字段的权重
        bqry = addFilters(bqry, req);
        // 给 filters 加上 boost
/*        if (bqry.isBool()) {
            final BoolQuery boolQuery = bqry.bool();
            bqry = Query.of(q -> q
                    .constantScore(cs -> cs
                            .filter(f -> f.bool(boolQuery)) // 原来的 bool query
                            .boost(0.05f)                   // 设置 boost

                    )
            );
        }*/

        // 3.设置高亮配置
        Highlight.Builder highlightBuilder = new Highlight.Builder();
        highlightBuilder
                .fields("content_ltks", new HighlightField.Builder().build())
                .fields("title_ltks", new HighlightField.Builder().build())
                .fragmentSize(0)
                .numberOfFragments(0)
                .boundaryScannerLocale("zh-CN")
                .boundaryScanner(BoundaryScanner.Sentence)
                .boundaryChars(",./;:\\!()，。？：！……（）——、");

        Highlight addhighlight = highlightBuilder.build();

        // 4. 构建 SearchRequest
        SearchRequest.Builder searchBuilder = new SearchRequest.Builder()
                .query(bqry) // 使用过滤后的 bqry
                .from(pg * ps)
                .size(ps)
                .highlight(addhighlight)
                .source(s -> s.filter(f -> f.includes(src)));

        SearchRequest searchRequest = searchBuilder.build();

        // 5. 转换 SearchRequest 为 Map (相当于 Python 的 to_dict())
        String json = gson.toJson(searchRequest);
        @SuppressWarnings("unchecked")
        Map<String, Object> sMap = gson.fromJson(json, Map.class);
        Map<String, Object> knn = _vector(question, similarity, topK);
        // filter = bqry
        String bqryJson = gson.toJson(bqry);
        @SuppressWarnings("unchecked")
        Map<String, Object> bqryMap = gson.fromJson(bqryJson, Map.class);
        knn.put("filter", bqryMap);

        sMap.put("knn", knn);

        // 如果不需要 highlight，可以删除
        boolean highlightEnabled = (boolean) req.getOrDefault("highlight", false);
        if (!highlightEnabled && sMap.containsKey("highlight")) {
            sMap.remove("highlight");
        }

        // 拿到 query_vector
        List<Float> qVec = (List<Float>) knn.get("query_vector");


        // 6. 执行搜索
        SearchResponse<Map> res = es.search(sMap, indexName, src, "30");

        // 使用占位符打印日志
        log.info("TOTAL: {}", es.getTotal(res));

        // keywords 扩展
        Set<String> kwds = new HashSet<>();
        for (String k : keywords) {
            kwds.add(k);
            String tokenized = tokenizer.fine_grained_tokenize(k);
            if (tokenized != null && !tokenized.isEmpty()) {
                String[] pieces = tokenized.split(" ");
                for (String kk : pieces) {
                    if (kk.length() < 2) continue;
                    if (kwds.contains(kk)) continue;
                    kwds.add(kk);
                }
            }
        }

        // 聚合结果
        List<Pair<String, Long>> aggs = getAggregation(res, "docnm_kwd");

        // 7. 解析结果
        return new SearchResult(
                es.getTotal(res),
                es.getDocIds(res),
                qVec,
                aggs,
                this.getHighlight(res, keywords, "content_with_weight"),
                this.getFields(res, src),
                new ArrayList<>(kwds)
        );
    }

    public static class SearchResult {
        public long total;
        public List<String> ids;
        public List<Float> qVec;
        public List<Pair<String, Long>> aggs;
        public Map<String, Object> highlight;
        public Map<String, Map<String, String>> fields;
        public List<String> keywords;

        public SearchResult(long total, List<String> ids,List<Float> qVec,List<Pair<String, Long>> aggs,Map<String, Object> highlight,
                            Map<String, Map<String, String>> fields, List<String> keywords) {
            this.total = total;
            this.ids = ids;
            this.qVec = qVec;
            this.aggs = aggs;
            this.highlight = highlight;
            this.fields = fields;
            this.keywords = keywords;
        }
    }

    public Map<String, Object> retriever(String question,
                                         Object embdMdl,
                                         List<String> kbIds,
                                         int page,
                                         int pageSize,
                                         double similarityThreshold,
                                         double vectorSimilarityWeight,
                                         int top,
                                         List<String> docIds,
                                         boolean aggs,
                                         Object rerankMdl,
                                         boolean highlight) throws Exception {

        Map<String, Object> ranks = new HashMap<>();
        ranks.put("total", 0);
        ranks.put("chunks", new ArrayList<Map<String, Object>>());
        ranks.put("doc_aggs", new ArrayList<Map<String, Object>>());

        if (question == null || question.trim().isEmpty()) {
            return ranks;
        }

        final int RERANK_PAGE_LIMIT = 3;

        Map<String, Object> req = new HashMap<>();
        req.put("kb_ids", kbIds);
        req.put("doc_ids", docIds);
        req.put("size", Math.max(pageSize * RERANK_PAGE_LIMIT, 50));
        req.put("question", question);
        req.put("vector", true);
        req.put("topk", top);
        req.put("similarity", similarityThreshold);
        req.put("available_int", 1);

        if (page > RERANK_PAGE_LIMIT) {
            req.put("page", page);
            req.put("size", pageSize);
        }

        // 1. 搜索 ES
        SearchResult sres = this.search(req, SEARCH_ES_INDEX, highlight);
        ranks.put("total", sres.total);
        System.out.println("检索后的结果: " + sres);
        // 2.重排序再次打分
        String cfield = "content_ltks";
        Double vtweight = 0.7;
        Double tkweight = 1.0 - vtweight;

        EsQueryer.HybridResult rerank_res = null;
        if (page <= RERANK_PAGE_LIMIT) {
            if (rerankMdl == null) {
                EsQueryer.HybridResult rerank = rerankByModel(rerankMdl, sres, question, vectorSimilarityWeight, tkweight);
                ranks.put("rerank_scores", 0.0);
                ranks.put("rerank_vector_scores", 0.0);
                ranks.put("rerank_token_scores", 0.0);
            } else {
                rerank_res = this.rerank(sres, tkweight, vtweight, cfield);
                System.out.println("最终结果: " + rerank_res);
            }
        }
        double[] sim = rerank_res.combined;
        double[] tksim = rerank_res.tokenSim;
        double[] vtsim = rerank_res.vecSim;

        // 排序 (sim * -1) 等价于按 sim 值降序
        int n = sim.length;
        Integer[] indices = IntStream.range(0, n)
                .boxed()
                .sorted((i, j) -> Double.compare(sim[j], sim[i])) // 降序
                .toArray(Integer[]::new);

        // 分页处理
        int from = Math.max(0, (page - 1) * pageSize);
        int to = Math.min(page * pageSize, indices.length);
        Integer[] idx = Arrays.copyOfRange(indices, from, to);

        List<Map<String, Object>> chunks = new ArrayList<>();
        ranks.put("chunks", chunks);
        Map<String, Map<String, Object>> docAggs = new HashMap<>();
        ranks.put("doc_aggs", docAggs);
        for (int i : idx) {
            if (sim[i] < similarityThreshold) {
                break;
            }
            if (chunks.size() >= pageSize) {
                if (aggs) continue;
                break;
            }

            String id = sres.ids.get(i);
            Map<String, String> field = sres.fields.get(id);

            String dnm = (String) field.getOrDefault("docnm_kwd", "faq");
            String did = (String) field.getOrDefault("doc_id", "");

            Map<String, Object> d = new HashMap<>();
            d.put("chunk_id", id);
            d.put("content_ltks", field.getOrDefault("content_ltks", ""));
            d.put("content_with_weight", field.getOrDefault("content_with_weight", ""));
            d.put("doc_id", did);
            d.put("docnm_kwd", dnm);
            d.put("kb_id", field.getOrDefault("kb_id", ""));
//            d.put("important_kwd", field.getOrDefault("important_kwd", new ArrayList<>()));
            d.put("important_kwd", field.getOrDefault("important_kwd", String.valueOf(new ArrayList<>())));
            d.put("similarity", sim[i]);
            d.put("vector_similarity", vtsim[i]);
            d.put("term_similarity", tksim[i]);

            // trans2floats
            String vecStr = (String) field.getOrDefault("q_" + "1024" + "_vec",
                    String.join("\t", Collections.nCopies(1024, "0")));
            d.put("vector", trans2floats(vecStr));

            d.put("parent_id", field.getOrDefault("parent_id", ""));
            d.put("title_tks", field.getOrDefault("title_tks", ""));
            d.put("faq_question", field.getOrDefault("faq_question", ""));

            if (highlight) {
                if (sres.highlight.containsKey(id)) {
                    d.put("highlight", rmSpace(String.valueOf(sres.highlight.get(id))));
                } else {
                    d.put("highlight", d.get("content_with_weight"));
                }
            }

            chunks.add(d);

            // 更新 doc_aggs
            docAggs.putIfAbsent(dnm, new HashMap<String, Object>() {{
                put("doc_id", did);
                put("count", 0);
            }});
            int count = (int) docAggs.get(dnm).get("count");
            docAggs.get(dnm).put("count", count + 1);
        }
        // 按 count 降序排列 doc_aggs
        List<Map<String, Object>> docAggList = docAggs.entrySet().stream()
                .sorted((e1, e2) -> Integer.compare(
                        (int) e2.getValue().get("count"),
                        (int) e1.getValue().get("count")))
                .map(e -> {
                    Map<String, Object> m = new HashMap<>();
                    m.put("doc_name", e.getKey());
                    m.put("doc_id", e.getValue().get("doc_id"));
                    m.put("count", e.getValue().get("count"));
                    return m;
                })
                .collect(Collectors.toList());

        ranks.put("doc_aggs", docAggList);
        return ranks;
    }

    public EsQueryer.HybridResult rerankByModel(Object mdl, SearchResult sres, String question, double tkWeight, double vtWeight) {
        return rerank(sres, tkWeight, vtWeight,"content-ltks");
    }

    public EsQueryer.HybridResult rerank(SearchResult sres, double tkWeight,double vtWeight , String cfield) {
        List<String> keywords = sres.keywords;
        List<String> ids = sres.ids;
        Map<String, Map<String, String>> fields = sres.fields;
        // 获取向量结果
        List<List<Float>> ins_embeded = new ArrayList<>();
        // 获取关键词
        List<List<String>> insTw = new ArrayList<>();
        // 遍历 ids，按顺序取 fields 的 value
        for (String id : ids) {
            if (fields.containsKey(id)) {
                // 获取向量数据
                String itemEmbedding = fields.get(id).get("q_1024_vec");
                List<Float> itemEmbeddingDealed = Dealer.trans2floats(itemEmbedding);
                ins_embeded.add(itemEmbeddingDealed);

                // 获取content_ltks
                String[] parts = fields.get(id).get(cfield).split("\\s+"); // "\\s+" 表示匹配一个或多个空格
                // 转成 List<String>（可选）
                List<String> tokens = Arrays.asList(parts);
                // 用 LinkedHashSet 去重并保持顺序
                Set<String> uniqueSet = new LinkedHashSet<>(tokens);
                // 转换为 ArrayList
                List<String> content_ltks = new ArrayList<>(uniqueSet);

                // 获取title_tks
                List<String> title_tks = new ArrayList<>();
                String[] title_t = fields.get(id).get("title_tks").split("\\s+");
                if (title_t.length > 0) {
                    title_tks.addAll(Arrays.asList(title_t));
                }
                //获取important_kwd
                List<String> importantKwd = new ArrayList<>();
                Map<String, String> fieldsMap = fields.get(id);
                String hasimportant_kwd = fields.get(id).get("important_kwd");
                if (hasimportant_kwd != null && !hasimportant_kwd.isEmpty()) {
                     String[] splitted =hasimportant_kwd.split("\t");
                     fieldsMap.put("important_kwd", Arrays.asList(splitted).toString());
                    // 遍历分割后的结果
                    for (String ikl : splitted) {
                        // 新增原始关键词
                        importantKwd.add(ikl);

                        // 调用 tokenizer 进行分词
                        try {
                            String tokenized = tokenizer.tokenize(ikl); // 返回字符串
                            if (tokenized != null && !tokenized.isEmpty()) {
                                importantKwd.addAll(Arrays.asList(tokenized.split(" ")));
                            }
                        } catch (Exception e) {
                            // 忽略分词异常
                        }
                    }
                }
                // 获取FAQ
                List<String> tks = new ArrayList<>();
                if (fields.get(id).get("docnm_kwd") != null && fields.get(id).get("docnm_kwd").equals("FAQ")){
                    // title_tks + important_kwd * 2
                    tks.addAll(title_tks);
                    tks.addAll(importantKwd);
                    tks.addAll(importantKwd); // 相当于 *2
                }else{
                    // content_ltks + title_tks + important_kwd * 2
                    tks.addAll(content_ltks);
                    tks.addAll(title_tks);
                    tks.addAll(importantKwd);
                    tks.addAll(importantKwd); // 相当于 *2
                }
                // append 到 ins_tw
                insTw.add(tks);
            }
        }

        // 假设 sres.qVec 是 ArrayList<Float>
        List<Float> qVecList = sres.qVec;

        // 转换为 Float[]
        Float[] qVecArray = qVecList.toArray(new Float[0]);
        EsQueryer.HybridResult rerank_res= this.qryr.hybridSimilarity(qVecArray, ins_embeded, keywords ,insTw, tkWeight, vtWeight);

        return rerank_res;
    }

}
