package TokenizerConvert;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

public class TermWeightDealer {

    private static final Logger logger = LoggerFactory.getLogger(TermWeightDealer.class);
    // 创建 RagTokenizer 类的实例
    RagTokenizer tokenizer = new RagTokenizer();
    private Set<String> stopWords;
    private Map<String, String> ne;
    private Map<String, Integer> df;

    public TermWeightDealer() {
        stopWords = new HashSet<>(Arrays.asList(
                "请问", "您", "你", "我", "他", "是", "的", "就", "有", "于", "及", "即", "在",
                "为", "最", "有", "从", "以", "了", "将", "与", "吗", "吧", "中", "#",
                "什么", "怎么", "哪个", "哪些", "啥", "相关"
        ));
        ne = new HashMap<>();
        df = new HashMap<>();

        String fnm = Paths.get(getProjectBaseDirectory(), "utils", "res").toString();
        try (Reader reader = new InputStreamReader(
                new FileInputStream(Paths.get(fnm, "ner.json").toFile()), "UTF-8")) {
            Gson gson = new Gson();
            Type type = new TypeToken<Map<String, String>>(){}.getType();
            ne = gson.fromJson(reader, type);
        } catch (Exception e) {
            logger.warn("Load ner.json FAIL!");
        }
    }

    private static String getProjectBaseDirectory() {
        File current = new File(TermWeightDealer.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        return current.getParentFile().getParentFile().getAbsolutePath();
    }

    public List<String> pretoken(String txt, boolean num, boolean stpwd) {
        List<String> patt = Collections.singletonList(
                "[~—\\t @#%!<>,\\.\\?\":;'\\{\\}\\[\\]_\\=\\(\\)\\|，。？》•●○↓《；‘’：“”【¥ 】…￥！、·（）×`&\\\\/「」\\\\]"
        );
        List<String> res = new ArrayList<>();

        String[] tokens = tokenizer.tokenize(txt).split("\\s+");
//        String[] tokens = tokenizer.tokenize(txt).split("\\s+");
        for (String t : tokens) {
            String tk = t;
            if ((stpwd && stopWords.contains(tk)) ||
                    (tk.matches("[0-9]$") && !num)) {
                continue;
            }
            for (String p : patt) {
                if (Pattern.matches(p, t)) {
                    tk = "#";
                    break;
                }
            }
            if (!"#".equals(tk) && !tk.isEmpty()) {
                res.add(tk);
            }
        }
        return res;
    }

    public List<String> tokenMerge(List<String> tks) {
        List<String> res = new ArrayList<>();
        int i = 0;
        while (i < tks.size()) {
            int j = i;
            if (i == 0 && oneTerm(tks.get(i)) &&
                    tks.size() > 1 &&
                    (tks.get(i + 1).length() > 1 &&
                            !tks.get(i + 1).matches("[0-9a-zA-Z]"))) {
                res.add(tks.get(0) + " " + tks.get(1));
                i = 2;
                continue;
            }
            while (j < tks.size() && tks.get(j) != null &&
                    !stopWords.contains(tks.get(j)) &&
                    oneTerm(tks.get(j))) {
                j++;
            }
            if (j - i > 1) {
                if (j - i < 5) {
                    res.add(String.join(" ", tks.subList(i, j)));
                    i = j;
                } else {
                    res.add(String.join(" ", tks.subList(i, i + 2)));
                    i = i + 2;
                }
            } else {
                if (tks.get(i).length() > 0) {
                    res.add(tks.get(i));
                }
                i++;
            }
        }
        return res;
    }

    private boolean oneTerm(String t) {
        return t.length() == 1 || t.matches("[0-9a-z]{1,2}$");
    }

    public String ner(String t) {
        if (ne == null || ne.isEmpty()) return "";
        return ne.getOrDefault(t, "");
    }

    public List<String> split(String txt) {
        List<String> tks = new ArrayList<>();
        String[] parts = txt.replaceAll("[ \\t]+", " ").split(" ");
        for (String t : parts) {
            if (!tks.isEmpty() &&
                    tks.get(tks.size() - 1).matches(".*[a-zA-Z]$") &&
                    t.matches(".*[a-zA-Z]$") &&
                    !"func".equals(ne.getOrDefault(t, "")) &&
                    !"func".equals(ne.getOrDefault(tks.get(tks.size() - 1), ""))) {
                String merged = tks.remove(tks.size() - 1) + " " + t;
                tks.add(merged);
            } else {
                tks.add(t);
            }
        }
        return tks;
    }

    public List<Map.Entry<String, Double>> weights(List<String> tks, boolean preprocess) {
        List<Map.Entry<String, Double>> tw = new ArrayList<>();
        // 定义内部函数
        java.util.function.Function<String, Double> nerFunc = (t) -> {
            if (t.matches("[0-9,.]{2,}$")) return 2.0;
            if (t.matches("[a-z]{1,2}$")) return 0.01;
            if (ne == null || !ne.containsKey(t)) return 1.0;

            Map<String, Double> m = new HashMap<>();
            m.put("toxic", 2.0);
            m.put("func", 1.0);
            m.put("corp", 3.0);
            m.put("loca", 3.0);
            m.put("sch", 3.0);
            m.put("stock", 3.0);
            m.put("firstnm", 1.0);

            return m.getOrDefault(ne.get(t), 1.0);
        };

        java.util.function.Function<String, Double> postagFunc = (t) -> {
            String tg = tokenizer.tag(t);
            if (Arrays.asList("r", "c", "d").contains(tg)) return 0.3;
            if (Arrays.asList("ns", "nt").contains(tg)) return 3.0;
            if ("n".equals(tg)) return 2.0;
            if (t.matches("[0-9-]+")) return 2.0;
            return 1.0;
        };

        java.util.function.Function<String, Double> freqFunc = new java.util.function.Function<String, Double>() {
            @Override
            public Double apply(String t) {
                if (t.matches("[0-9. -]{2,}$")) return 3.0;
                Double s = tokenizer.freq(t);
                if (s == null && t.matches("[a-z. -]+$")) return 300.0;
                if (s == null) s = 0.0;

                if (s == 0.0 && t.length() >= 4) {
                    String[] subs = tokenizer.fine_grained_tokenize(t).split("\\s+");
                    List<String> valid = new ArrayList<>();
                    for (String tt : subs) {
                        if (tt.length() > 1) valid.add(tt);
                    }
                    if (valid.size() > 1) {
                        double minVal = valid.stream().map(this).min(Double::compare).orElse(0.0);
                        s = minVal / 6.0;
                    } else {
                        s = 0.0;
                    }
                }
                return Math.max(s, 10.0);
            }
        };

        java.util.function.Function<String, Double> dfFunc = new java.util.function.Function<String, Double>() {
            @Override
            public Double apply(String t) {
                if (t.matches("[0-9. -]{2,}$")) return 5.0;
                if (df.containsKey(t)) return df.get(t) + 3.0;
                if (t.matches("[a-z. -]+$")) return 300.0;
                if (t.length() >= 4) {
                    String[] subs = tokenizer.fine_grained_tokenize(t).split("\\s+");
                    List<String> valid = new ArrayList<>();
                    for (String tt : subs) {
                        if (tt.length() > 1) valid.add(tt);
                    }
                    if (valid.size() > 1) {
                        double minVal = valid.stream().map(this).min(Double::compare).orElse(3.0);
                        return Math.max(3.0, minVal / 6.0);
                    }
                }
                return 3.0;
            }
        };

        java.util.function.BiFunction<Double, Double, Double> idf = (s, N) ->
                Math.log10(10.0 + ((N - s + 0.5) / (s + 0.5)));

        if (!preprocess) {
            double[] idf1 = tks.stream().mapToDouble(t -> idf.apply(freqFunc.apply(t), 10000000.0)).toArray();
            double[] idf2 = tks.stream().mapToDouble(t -> idf.apply(dfFunc.apply(t), 1000000000.0)).toArray();
            double[] factors = tks.stream().mapToDouble(t -> nerFunc.apply(t) * postagFunc.apply(t)).toArray();

            for (int i = 0; i < tks.size(); i++) {
                double w = (0.3 * idf1[i] + 0.7 * idf2[i]) * factors[i];
                tw.add(new AbstractMap.SimpleEntry<>(tks.get(i), w));
            }
        } else {
            for (String tk : tks) {
                List<String> tt = tokenMerge(pretoken(tk, true, true));
                double[] idf1 = tt.stream().mapToDouble(t -> idf.apply(freqFunc.apply(t), 10000000.0)).toArray();
                double[] idf2 = tt.stream().mapToDouble(t -> idf.apply(dfFunc.apply(t), 1000000000.0)).toArray();
                double[] factors = tt.stream().mapToDouble(t -> nerFunc.apply(t) * postagFunc.apply(t)).toArray();

                for (int i = 0; i < tt.size(); i++) {
                    double w = (0.3 * idf1[i] + 0.7 * idf2[i]) * factors[i];
                    tw.add(new AbstractMap.SimpleEntry<>(tt.get(i), w));
                }
            }
        }

        // 归一化
        double sum = tw.stream().mapToDouble(Map.Entry::getValue).sum();
        List<Map.Entry<String, Double>> result = new ArrayList<>();
        for (Map.Entry<String, Double> e : tw) {
            result.add(new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue() / sum));
        }
        return result;
    }

    // 重载方法：提供默认值 true
    public List<Map.Entry<String, Double>> weights(List<String> tks) {
        return weights(tks, true); // 调用主方法，preprocess 默认为 true
    }


}
