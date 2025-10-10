//这个是没有用的文件
package TokenizerConvert;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.*;

import co.elastic.clients.json.JsonData;

import java.util.*;

public class QueryBuilderUtil {

    /**
     * 将 Python 风格的 dict 查询体(Map) 转换为 Elasticsearch Query 对象
     */
    public static Query fromMap(Map<String, Object> q) {
        if (q == null || q.isEmpty()) {
            return null;
        }

        Object queryObj = q.get("query");
        if (!(queryObj instanceof Map)) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> queryMap = (Map<String, Object>) queryObj;

        return parseQuery(queryMap);
    }

    private static Query parseQuery(Map<String, Object> q) {
        if (q.containsKey("bool")) {
            return parseBoolQuery((Map<String, Object>) q.get("bool"));
        } else if (q.containsKey("term")) {
            return parseTermQuery((Map<String, Object>) q.get("term"));
        } else if (q.containsKey("terms")) {
            return parseTermsQuery((Map<String, Object>) q.get("terms"));
        } else if (q.containsKey("range")) {
            return parseRangeQuery((Map<String, Object>) q.get("range"));
        } else if (q.containsKey("query_string")) {
            return parseQueryStringQuery((Map<String, Object>) q.get("query_string"));
        } else if (q.containsKey("script_score")) {
            return parseScriptScoreQuery((Map<String, Object>) q.get("script_score"));
        }
        throw new IllegalArgumentException("Unsupported query type: " + q);
    }

    private static Query parseBoolQuery(Map<String, Object> map) {
        BoolQuery.Builder b = new BoolQuery.Builder();

        if (map.containsKey("must")) {
            List<Map<String, Object>> mustList = (List<Map<String, Object>>) map.get("must");
            for (Map<String, Object> sub : mustList) {
                b.must(parseQuery(sub));
            }
        }

        if (map.containsKey("filter")) {
            List<Map<String, Object>> filterList = (List<Map<String, Object>>) map.get("filter");
            for (Map<String, Object> sub : filterList) {
                b.filter(parseQuery(sub));
            }
        }

        if (map.containsKey("must_not")) {
            List<Map<String, Object>> mustNotList = (List<Map<String, Object>>) map.get("must_not");
            for (Map<String, Object> sub : mustNotList) {
                b.mustNot(parseQuery(sub));
            }
        }

        return new Query(b.build());
    }

    private static Query parseTermQuery(Map<String, Object> map) {
        String field = map.keySet().iterator().next();
        Object value = map.get(field);
        return TermQuery.of(t -> t.field(field).value((FieldValue) JsonData.of(value)))._toQuery();
    }

    private static Query parseTermsQuery(Map<String, Object> map) {
        String field = map.keySet().iterator().next();
        List<Object> values = (List<Object>) map.get(field);

        List<FieldValue> fv = new ArrayList<>();
        for (Object v : values) {
            fv.add(FieldValue.of(v.toString()));
        }
        return TermsQuery.of(t -> t.field(field).terms(ts -> ts.value(fv)))._toQuery();
    }

    private static Query parseRangeQuery(Map<String, Object> map) {
        String field = map.keySet().iterator().next();
        @SuppressWarnings("unchecked")
        Map<String, Object> cond = (Map<String, Object>) map.get(field);

        RangeQuery.Builder r = new RangeQuery.Builder().field(field);
        if (cond.containsKey("lt")) {
            r.lt(JsonData.of(cond.get("lt")));
        }
        if (cond.containsKey("lte")) {
            r.lte(JsonData.of(cond.get("lte")));
        }
        if (cond.containsKey("gt")) {
            r.gt(JsonData.of(cond.get("gt")));
        }
        if (cond.containsKey("gte")) {
            r.gte(JsonData.of(cond.get("gte")));
        }
        return r.build()._toQuery();
    }

    private static Query parseQueryStringQuery(Map<String, Object> map) {
        QueryStringQuery.Builder qs = new QueryStringQuery.Builder();
        if (map.containsKey("query")) {
            qs.query(map.get("query").toString());
        }
        if (map.containsKey("fields")) {
            List<String> fields = (List<String>) map.get("fields");
            qs.fields(fields);
        }
        if (map.containsKey("boost")) {
            qs.boost(Float.parseFloat(map.get("boost").toString()));
        }
        if (map.containsKey("minimum_should_match")) {
            qs.minimumShouldMatch(map.get("minimum_should_match").toString());
        }
        return qs.build()._toQuery();
    }

    private static Query parseScriptScoreQuery(Map<String, Object> map) {
        // 这里用 ScriptScoreQuery 代替 knn 查询
        ScriptScoreQuery.Builder ssb = new ScriptScoreQuery.Builder();
        if (map.containsKey("query")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> subQuery = (Map<String, Object>) map.get("query");
            ssb.query(parseQuery(subQuery));
        }
        if (map.containsKey("script")) {
            // 这里假设 script 是 Map，里面 source 和 params
            @SuppressWarnings("unchecked")
            Map<String, Object> scriptMap = (Map<String, Object>) map.get("script");
            String source = scriptMap.get("source").toString();
            @SuppressWarnings("unchecked")
//            Map<String, Object> params = (Map<String, Object>) scriptMap.get("params");
            Map<String, Object> rawParams = (Map<String, Object>) scriptMap.get("params");

            Map<String, JsonData> jsonParams = new HashMap<>();
            if (rawParams != null) {
                for (Map.Entry<String, Object> e : rawParams.entrySet()) {
                    jsonParams.put(e.getKey(), JsonData.of(e.getValue()));
                }
            }

            String addsource = scriptMap.get("source").toString();

            ssb.script(s -> s.inline(i -> i.source(addsource).params(jsonParams)));
//            ssb.script(s -> s.inline(i -> i.source(source).params(params)));
        }
        return new Query(ssb.build());
    }
}

//public class QueryBuilderUtil {
//
//    /**
//     * 将 Python 风格的 dict 查询体(Map) 转换为 Elasticsearch Query 对象
//     */
//    public static Query fromMap(Map<String, Object> q) {
//        if (q == null || q.isEmpty()) {
//            return null;
//        }
//
//        // 只取 "query" 部分
//        Object queryObj = q.get("query");
//        if (!(queryObj instanceof Map)) {
//            return null;
//        }
//        @SuppressWarnings("unchecked")
//        Map<String, Object> queryMap = (Map<String, Object>) queryObj;
//
//        return parseQuery(queryMap);
//    }
//
//    private static Query parseQuery(Map<String, Object> q) {
//        if (q.containsKey("bool")) {
//            return parseBoolQuery((Map<String, Object>) q.get("bool"));
//        } else if (q.containsKey("term")) {
//            return parseTermQuery((Map<String, Object>) q.get("term"));
//        } else if (q.containsKey("terms")) {
//            return parseTermsQuery((Map<String, Object>) q.get("terms"));
//        } else if (q.containsKey("range")) {
//            return parseRangeQuery((Map<String, Object>) q.get("range"));
//        } else if (q.containsKey("query_string")) {
//            return parseQueryStringQuery((Map<String, Object>) q.get("query_string"));
//        }
//        throw new IllegalArgumentException("Unsupported query type: " + q);
//    }
//
//    private static Query parseBoolQuery(Map<String, Object> map) {
//        BoolQuery.Builder b = new BoolQuery.Builder();
//
//        if (map.containsKey("must")) {
//            List<Map<String, Object>> mustList = (List<Map<String, Object>>) map.get("must");
//            for (Map<String, Object> sub : mustList) {
//                b.must(parseQuery(sub));
//            }
//        }
//
//        if (map.containsKey("filter")) {
//            List<Map<String, Object>> filterList = (List<Map<String, Object>>) map.get("filter");
//            for (Map<String, Object> sub : filterList) {
//                b.filter(parseQuery(sub));
//            }
//        }
//
//        if (map.containsKey("must_not")) {
//            List<Map<String, Object>> mustNotList = (List<Map<String, Object>>) map.get("must_not");
//            for (Map<String, Object> sub : mustNotList) {
//                b.mustNot(parseQuery(sub));
//            }
//        }
//
//        return new Query(b.build());
//    }
//
//    private static Query parseTermQuery(Map<String, Object> map) {
//        String field = map.keySet().iterator().next();
//        Object value = map.get(field);
//        return TermQuery.of(t -> t.field(field).value((FieldValue) JsonData.of(value)))._toQuery();
//    }
//
//    private static Query parseTermsQuery(Map<String, Object> map) {
//        String field = map.keySet().iterator().next();
//        List<Object> values = (List<Object>) map.get(field);
//
//        List<FieldValue> fv = new ArrayList<>();
//        for (Object v : values) {
//            fv.add(FieldValue.of(v.toString()));
//        }
//        return TermsQuery.of(t -> t.field(field).terms(ts -> ts.value(fv)))._toQuery();
//    }
//
//    private static Query parseRangeQuery(Map<String, Object> map) {
//        String field = map.keySet().iterator().next();
//        @SuppressWarnings("unchecked")
//        Map<String, Object> cond = (Map<String, Object>) map.get(field);
//
//        RangeQuery.Builder r = new RangeQuery.Builder().field(field);
//        if (cond.containsKey("lt")) {
//            r.lt(JsonData.of(cond.get("lt")));
//        }
//        if (cond.containsKey("lte")) {
//            r.lte(JsonData.of(cond.get("lte")));
//        }
//        if (cond.containsKey("gt")) {
//            r.gt(JsonData.of(cond.get("gt")));
//        }
//        if (cond.containsKey("gte")) {
//            r.gte(JsonData.of(cond.get("gte")));
//        }
//        return r.build()._toQuery();
//    }
//
//    private static Query parseQueryStringQuery(Map<String, Object> map) {
//        QueryStringQuery.Builder qs = new QueryStringQuery.Builder();
//        if (map.containsKey("query")) {
//            qs.query(map.get("query").toString());
//        }
//        if (map.containsKey("fields")) {
//            List<String> fields = (List<String>) map.get("fields");
//            qs.fields(fields);
//        }
//        if (map.containsKey("boost")) {
//            qs.boost(Float.parseFloat(map.get("boost").toString()));
//        }
//        if (map.containsKey("minimum_should_match")) {
//            qs.minimumShouldMatch(map.get("minimum_should_match").toString());
//        }
//        return qs.build()._toQuery();
//    }
//
//}

