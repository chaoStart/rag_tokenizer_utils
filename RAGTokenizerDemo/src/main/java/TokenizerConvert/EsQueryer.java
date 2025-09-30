package TokenizerConvert;

import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.TextQueryType;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Locale;
import java.text.DecimalFormat;

public class EsQueryer {
    private TermWeightDealer tw;
    private Object es; // 这里你可以替换成 ElasticsearchClient
    private SynonymDealer syn;
    private RagTokenizer tokenizer = new RagTokenizer();
    public List<String> flds = Arrays.asList("ask_tks^10", "ask_small_tks");

    public EsQueryer(Object es) {
        this.tw = new TermWeightDealer();
        this.es = es;
        this.syn = new SynonymDealer();
        this.flds = Arrays.asList("ask_tks^10", "ask_small_tks");
    }

    // ---- 简单 Pair 实用类（返回 Query + keywords 列表） ----
    public static class Pair<F, S> {
        private final F bqry;
        private final S keywords;
        public Pair(F f, S s) { this.bqry = f; this.keywords = s; }
        public F getbqry() { return bqry; }
        public S getkeywords() { return keywords; }
    }

    public static String subSpecialChar(String line) {
        return line.replaceAll("([:\\{\\}/\\[\\]\\-\\*\\\"\\(\\)\\|\\+~\\^])", "\\\\$1").trim();
    }

    public static boolean isChinese(String line) {
        String[] arr = line.split("[ \t]+");
        if (arr.length <= 3) {
            return true;
        }
        int e = 0;
        for (String t : arr) {
            if (!t.matches("[a-zA-Z]+$")) {
                e++;
            }
        }
        return e * 1.0 / arr.length >= 0.7;
    }

    public static String rmWWW(String txt) {
        String[][] patts = new String[][]{
                { "是*(什么样的|哪家|一下|那家|请问|啥样|咋样了|什么时候|何时|何地|何人|是否|是不是|多少|哪里|怎么|哪儿|怎么样|如何|哪些|是啥|啥是|啊|吗|呢|吧|咋|什么|有没有|呀)是*", "" },
                { "(^| )(what|who|how|which|where|why)('re|'s)? ", " " },
                { "(^| )('s|'re|is|are|were|was|do|does|did|don't|doesn't|didn't|has|have|be|there|you|me|your|my|mine|just|please|may|i|should|would|wouldn't|will|won't|done|go|for|with|so|the|a|an|by|i'm|it's|he's|she's|they|they're|you're|as|by|on|in|at|up|out|down|of) ", " "}
        };
        for (String[] rp : patts) {
            txt = txt.replaceAll("(?i)" + rp[0], rp[1]);
        }
        return txt;
    }

    // ---------- 主要方法：question ----------
    /**
     * 返回 Pair<Query, List<String>>，Query 是对 ES 的查询对象（bool must 包着 query_string），
     * 第二个元素是关键词列表（去重）。
     *
     * 注意：此方法尽量与 Python 实现保持同样的 query 构造逻辑（使用 QueryStringQuery.of(...)._toQuery()）
     */
    public Pair<Query, List<String>> question(String txt) {
        return question(txt, "qa", "60%");
    }

    public Pair<Query, List<String>> question(String txt, String tbl, String minMatch) {
        if (txt == null) txt = "";
        // 规范化：小写、全角->半角、繁->简、去掉一些分隔符
        txt = tokenizer._tradi2simp(tokenizer._strQ2B(txt.toLowerCase())).trim();
        txt = txt.replaceAll("[ :\\r\\n\\t,，。？?/`!！&\\^%]+", " ");
        txt = rmWWW(txt).trim();

        // 非中文分支（英文/混合）——用 token 加权并构造 query_string
        if (!isChinese(txt)) {
            String[] tokens = tokenizer.tokenize(txt).split("\\s+");
            List<Map.Entry<String, Double>> tks_w = tw.weights(Arrays.asList(tokens));

            // 按照 Python 的多步过滤逻辑依次处理 token
            List<Map.Entry<String, Double>> filtered = new ArrayList<>();
            for (Map.Entry<String, Double> en : tks_w) {
                String tk = en.getKey();
                double w = en.getValue();
                if (tk == null) continue;
                tk = tk.replaceAll("[ \\\"'^]", "");
                tk = tk.replaceAll("^[a-z0-9]$", "");
                tk = tk.replaceAll("^[\\+\\-]", "");
                if (tk.isEmpty()) continue;
                filtered.add(new AbstractMap.SimpleEntry<>(tk, w));
            }

            // build q token list like ["term^0.1234", "\"a b\"^0.1234", ...]
            List<String> qtokens = new ArrayList<>();
            DecimalFormat df = new DecimalFormat("0.0000");
            for (Map.Entry<String, Double> en : filtered) {
                qtokens.add(String.format(Locale.ROOT, "%s^%s", en.getKey(), df.format(en.getValue())));
            }
            for (int i = 1; i < filtered.size(); i++) {
                String a = filtered.get(i - 1).getKey();
                String b = filtered.get(i).getKey();
                double wa = filtered.get(i - 1).getValue();
                double wb = filtered.get(i).getValue();
                double w = Math.max(wa, wb) * 2.0;
                qtokens.add(String.format(Locale.ROOT, "\"%s %s\"^%s", a, b, df.format(w)));
            }
            if (qtokens.isEmpty()) {
                qtokens.add(txt);
            }

            String queryString = String.join(" ", qtokens);

            // 构造 QueryStringQuery（注意：使用 QueryStringQuery.of(...)._toQuery()；然后把它放进 bool.must）
            Query qsQuery = QueryStringQuery.of(q -> q
                    .fields(flds)
                    .query(queryString)
                    .type(TextQueryType.BestFields)         // 与 python 等价的 best_fields
                    .minimumShouldMatch(minMatch)
            )._toQuery();

            Query finalQuery = Query.of(q -> q.bool(b -> b.must(qsQuery)));

            // 构造返回的关键字集合（去重）
            Set<String> kws = Arrays.stream(txt.split("\\s+")).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
            List<String> keywords = new ArrayList<>(kws);
            return new Pair<>(finalQuery, keywords);
        }

        // 中文分支：更复杂的构造，按 python 逻辑处理：
        List<String> qs = new ArrayList<>();
        Set<String> keywordsSet = new LinkedHashSet<>();

        List<String> splits = tw.split(txt);
        int limit = Math.min(256, splits.size());
        for (int idx = 0; idx < limit; idx++) {
            String tt = splits.get(idx);
            if (tt == null || tt.trim().isEmpty()) continue;
            keywordsSet.add(tt);

            // tw.weights 接收 List<String>，并返回 List<Entry<String, Double>>
            List<Map.Entry<String, Double>> twts = tw.weights(Arrays.asList(tt));
            List<String> syns = syn.lookup(tt,8);
            if (syns != null && !syns.isEmpty()) {
                keywordsSet.addAll(syns);
            }

            List<String> tmsParts = new ArrayList<>();
            // 按权重降序遍历
            List<Map.Entry<String, Double>> sorted = new ArrayList<>(twts);
            sorted.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

            for (Map.Entry<String, Double> en : sorted) {
                String tk = en.getKey();
                double w = en.getValue();

                // need_fine_grained_tokenize 判断
                boolean needFine = needFineGrainedTokenize(tk);

                List<String> sm = new ArrayList<>();
                if (needFine) {
                    String fine = tokenizer.fine_grained_tokenize(tk);
                    if (fine != null && !fine.trim().isEmpty()) {
                        for (String m : fine.split("\\s+")) {
                            // 清洗标点
                            String cleaned = m.replaceAll("[ ,\\./;\\'\\[\\]\\\\`~!@#$%\\^&\\*\\(\\)=\\+_<>\\?:\\\"\\{\\}\\|，。；‘’【】、！￥……（）——《》？：“”-]+", "");
                            if (cleaned.length() > 1) {
                                cleaned = subSpecialChar(cleaned);
                                if (cleaned.length() > 1) sm.add(cleaned);
                            }
                        }
                    }
                }

                keywordsSet.add(tk.replaceAll("[ \\\\\\\"']+", "")); // 加入关键词
                keywordsSet.addAll(sm);

                if (keywordsSet.size() >= 12) break;

                List<String> tkSyns = syn.lookup(tk,8);
                String tkEsc = subSpecialChar(tk);
                if (tkEsc.contains(" ")) {
                    tkEsc = "\"" + tkEsc + "\"";
                }
                if (tkSyns != null && !tkSyns.isEmpty()) {
                    tkEsc = "(" + tkEsc + " " + String.join(" ", tkSyns) + ")";
                }
                if (!sm.isEmpty()) {
                    String smJoin = String.join(" ", sm);
                    tkEsc = tkEsc + " OR \"" + smJoin + "\" OR (\"" + smJoin + "\"~2)^0.5";
                }

                if (tkEsc.trim().length() > 0) {
                    // 用 (tk)^w 的形式加入
                    tmsParts.add(String.format(Locale.ROOT, "(%s)^%.4f", tkEsc, w));
                }
            } // end each tk

            String tms = String.join(" ", tmsParts);

            if (twts.size() > 1) {
                String allT = sortedKeysString(sortedEntries(sorted));
                tms += String.format(Locale.ROOT, " (\"%s\"~4)^1.5", allT);
            }

            if (tt.matches("[0-9a-z ]+$")) {
                // 如果 tt 由数字/小写字母/空格构成
                String tokenized = tokenizer.tokenize(tt);
                tms = String.format(Locale.ROOT, "(\"%s\" OR \"%s\")", tt, tokenized);
            }

            String synsStr = "";
            if (syns != null && !syns.isEmpty()) {
                synsStr = syns.stream()
                        .map(s -> "\"" + subSpecialChar(tokenizer.tokenize(s)) + "\"^0.7")
                        .collect(Collectors.joining(" OR "));
            }
            if (!synsStr.isEmpty()) {
                tms = "(" + tms + ")^5 OR (" + synsStr + ")^0.7";
            }

            qs.add(tms);

            if (keywordsSet.size() >= 12) break;
        } // end for splits

        // 将 qs 用 OR 串起，做为 query_string 的 query
        String joined = qs.stream().filter(s -> s != null && !s.isEmpty()).map(s -> "(" + s + ")").collect(Collectors.joining(" OR "));
        if (joined.isEmpty()) joined = txt;

        String finalJoined = joined;
        Query qsQuery = QueryStringQuery.of(q -> q
                .fields(flds)
                .query(finalJoined)
                .type(TextQueryType.BestFields)
                .boost(1.0f)
                .minimumShouldMatch(minMatch)
        )._toQuery();

        List<Query> musts = new ArrayList<>();
        musts.add(qsQuery);

        Query finalQuery = Query.of(q -> q.bool(b -> b.must(musts)));

        List<String> keywords = new ArrayList<>(keywordsSet);
        return new Pair<>(finalQuery, keywords);
    }

    // ---------- 其他辅助方法 ----------

    private static boolean needFineGrainedTokenize(String tk) {
        if (tk == null) return false;
        if (tk.length() < 3) return false;
        if (tk.matches("[0-9a-z\\.\\+#_\\*-]+$")) return false;
        return true;
    }

    private static String sortedKeysString(List<Map.Entry<String, Double>> list) {
        return list.stream().map(Map.Entry::getKey).collect(Collectors.joining(" "));
    }

    private static List<Map.Entry<String, Double>> sortedEntries(List<Map.Entry<String, Double>> list) {
        List<Map.Entry<String, Double>> copy = new ArrayList<>(list);
        copy.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
        return copy;
    }

    // ---------- 相似度相关方法（Java 实现） ----------

    public static class HybridResult {
        public final double[] combined;
        public final double[] tokenSim;
        public final double[] vecSim;
        public HybridResult(double[] combined, double[] tokenSim, double[] vecSim) {
            this.combined = combined;
            this.tokenSim = tokenSim;
            this.vecSim = vecSim;
        }
    }

    /**
     * hybridSimilarity：把向量相似度（cosine）和 token-based 相似度加权合成
     * @param avec 单个查询向量
     * @param bvecs 目标向量矩阵（每行一个向量）
     * @param atks 查询 token（已经分好词的 List<String>）
     * @param btkss 目标 token 列表（List<List<String>>）
     * @param tkweight token 相似度权重
     * @param vtweight 向量相似度权重
     * @return HybridResult（三个数组：combined、tokenSim、vecSim）
     */
    public HybridResult hybridSimilarity(Float[] avec, List<List<Float>> bvecs, List<String> atks, List<List<String>> btkss, double tkweight, double vtweight) {
        // 1) 向量余弦相似度
        int n = bvecs.size();
        double[] vecSims = new double[n];
        for (int i = 0; i < n; i++) {
            vecSims[i] = cosineSimilarity(avec, bvecs.get(i));
        }

        // 2) token similarity
        double[] tksim = tokenSimilarity(atks, btkss);

        // 3) combine
        double[] combined = new double[n];
        for (int i = 0; i < n; i++) {
            combined[i] = vecSims[i] * vtweight + tksim[i] * tkweight;
        }
        return new HybridResult(combined, tksim, vecSims);
    }

//    public HybridResult hybridSimilarity(double[] avec, double[][] bvecs, List<String> atks, List<List<String>> btkss) {
//        return hybridSimilarity(avec, bvecs, atks, btkss, 0.3, 0.7);
//    }

//    private static double cosineSimilarity(Float[] a, Float[] b) {
//        if (a == null || b == null) return 0.0;
//        int len = Math.min(a.length, b.length);
//        double dot = 0.0, na = 0.0, nb = 0.0;
//        for (int i = 0; i < len; i++) {
//            dot += a[i] * b[i];
//            na += a[i] * a[i];
//            nb += b[i] * b[i];
//        }
//        if (na <= 0 || nb <= 0) return 0.0;
//        return dot / (Math.sqrt(na) * Math.sqrt(nb));
//    }
    private static double cosineSimilarity(Float[] a, List<Float> b) {
        if (a == null || b == null) return 0.0;
        int len = Math.min(a.length, b.size());
        double dot = 0.0, na = 0.0, nb = 0.0;
        for (int i = 0; i < len; i++) {
            dot += a[i] * b.get(i);
            na += a[i] * a[i];
            nb += b.get(i) * b.get(i);
        }
        if (na <= 0 || nb <= 0) return 0.0;
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }

    /**
     * tokenSimilarity: atks (查询 tokens) 与多个文档 tokens 列表 btkss 做相似度打分
     * 返回长度等于 btkss.size() 的相似度数组
     */
    public double[] tokenSimilarity(List<String> atks, List<List<String>> btkss) {
        Map<String, Double> aDict = toDict(atks);
        int n = btkss.size();
        double[] res = new double[n];
        for (int i = 0; i < n; i++) {
            res[i] = similarity(aDict, toDict(btkss.get(i)));
        }
        return res;
    }

    private Map<String, Double> toDict(List<String> tks) {
        Map<String, Double> d = new HashMap<>();
        if (tks == null) return d;
        long startTime = System.nanoTime();
        List<Map.Entry<String, Double>> wts = tw.weights(tks);
        for (Map.Entry<String, Double> en : wts) {
            String t = en.getKey();
            double c = en.getValue();
            d.put(t, d.getOrDefault(t, 0.0) + c);
        }
        long endTime = System.nanoTime();
        long duration = endTime - startTime; // 纳秒
        System.out.println("函数执行耗时: " + duration / 1_000_000.0 + " 毫秒");
        return d;
    }

    /**
     * similarity: qtwt (query token-weight map) 与 dtwt (doc token-weight map)
     * 基于 Python 逻辑：相同 token 累加 query 的权重 / query 总权重
     */
    public double similarity(Map<String, Double> qtwt, Map<String, Double> dtwt) {
        if (qtwt == null || qtwt.isEmpty()) return 0.0;
        double s = 1e-9;
        for (Map.Entry<String, Double> en : qtwt.entrySet()) {
            String k = en.getKey();
            double v = en.getValue();
            if (dtwt != null && dtwt.containsKey(k)) {
                s += v;
            }
        }
        double q = 1e-9;
        for (double v : qtwt.values()) q += v;
        return s / q;
    }
}