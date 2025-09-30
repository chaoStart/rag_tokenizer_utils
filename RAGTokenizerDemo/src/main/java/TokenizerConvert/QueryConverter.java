package TokenizerConvert;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.json.JsonData;

import java.util.*;

public class QueryConverter {

    public static Query parseQuery(Map<String, Object> map) {
        String kind = (String) map.get("_kind");
        Map<String, Object> value = (Map<String, Object>) map.get("_value");

        switch (kind) {
            case "Bool":
                return parseBoolQuery(value);
            case "Terms":
                return parseTermsQuery(value);
            case "Range":
                return parseRangeQuery(value);
            case "QueryString":
                return parseQueryString(value);
            case "ConstantScore":
                return parseConstantScore(value);
            case "Knn":
                return parseKnn(value);
            default:
                throw new IllegalArgumentException("Unsupported kind: " + kind);
        }
    }

    private static Query parseBoolQuery(Map<String, Object> value) {
        BoolQuery.Builder b = new BoolQuery.Builder();
//        parseSubQueries(b.must(), (List<Map<String, Object>>) value.get("must"));
//        parseSubQueries(b.filter(), (List<Map<String, Object>>) value.get("filter"));
//        parseSubQueries(b.should(), (List<Map<String, Object>>) value.get("should"));
//        parseSubQueries(b.mustNot(), (List<Map<String, Object>>) value.get("mustNot"));
//        return new Query(b.build());

        List<Map<String, Object>> mustList = (List<Map<String, Object>>) value.get("must");
        if (mustList != null) {
            for (Map<String, Object> sub : mustList) {
                b.must(parseQuery(sub));  // parseQuery 返回 Query
            }
        }
        List<Map<String, Object>> filterList = (List<Map<String, Object>>) value.get("filter");
        if (filterList != null) {
            for (Map<String, Object> sub : filterList) {
                b.filter(parseQuery(sub));
            }
        }

        List<Map<String, Object>> shouldList = (List<Map<String, Object>>) value.get("should");
        if (shouldList != null) {
            for (Map<String, Object> sub : shouldList) {
                b.should(parseQuery(sub));
            }
        }

        List<Map<String, Object>> mustNotList = (List<Map<String, Object>>) value.get("mustNot");
        if (mustNotList != null) {
            for (Map<String, Object> sub : mustNotList) {
                b.mustNot(parseQuery(sub));
            }
        }
        return new Query(b.build());
    }

    private static void parseSubQueries(List<Query> target, List<Map<String, Object>> raw) {
        if (raw != null) {
            for (Map<String, Object> q : raw) {
                target.add(parseQuery(q));
            }
        }
    }

    private static Query parseTermsQuery(Map<String, Object> value) {
        String field = (String) value.get("field");
        Map<String, Object> termsMap = (Map<String, Object>) value.get("terms");
        List<Map<String, Object>> termVals = (List<Map<String, Object>>) termsMap.get("_value");

        List<FieldValue> fvList = new ArrayList<>();
        for (Map<String, Object> term : termVals) {
            String v = (String) term.get("_value");
            fvList.add(FieldValue.of(v));
        }
        return TermsQuery.of(t -> t.field(field).terms(tv -> tv.value(fvList)))._toQuery();
    }

    private static Query parseRangeQuery(Map<String, Object> value) {
        String field = (String) value.get("field");
        RangeQuery.Builder rb = new RangeQuery.Builder().field(field);

        if (value.containsKey("lt")) {
            Map<String, Object> lt = (Map<String, Object>) value.get("lt");
            rb.lt(JsonData.of(lt.get("value")));
        }
        return new Query(rb.build());
    }

    private static Query parseQueryString(Map<String, Object> value) {
        QueryStringQuery.Builder qb = new QueryStringQuery.Builder()
                .query((String) value.get("query"))
                .defaultOperator(Operator.Or);

        if (value.containsKey("fields")) {
            List<String> fields = (List<String>) value.get("fields");
            qb.fields(fields);
        }
        if (value.containsKey("minimumShouldMatch")) {
            qb.minimumShouldMatch((String) value.get("minimumShouldMatch"));
        }
        return new Query(qb.build());
    }

    private static Query parseConstantScore(Map<String, Object> value) {
        ConstantScoreQuery.Builder cb = new ConstantScoreQuery.Builder();
        if (value.containsKey("filter")) {
            cb.filter(parseQuery((Map<String, Object>) value.get("filter")));
        }
        if (value.containsKey("boost")) {
            cb.boost(Float.parseFloat(value.get("boost").toString()));
        }
        return new Query(cb.build());
    }

    private static Query parseKnn(Map<String, Object> value) {
        // 在 ES Java Client 8.x，KNN 是通过 knn 查询字段，而不是标准 Query
        // 所以我们这里返回一个 MatchAll 或 ConstantScore 占位
        return MatchAllQuery.of(m -> m)._toQuery();
    }
}
