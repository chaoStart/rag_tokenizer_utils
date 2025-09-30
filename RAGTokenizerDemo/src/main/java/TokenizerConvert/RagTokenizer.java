package TokenizerConvert;

import co.elastic.clients.elasticsearch.indices.analyze.AnalyzeToken;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.github.houbb.opencc4j.util.ZhConverterUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.transport.Transport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.elasticsearch.indices.AnalyzeRequest;
import co.elastic.clients.elasticsearch.indices.AnalyzeResponse;
import co.elastic.clients.elasticsearch.core.*;
import org.elasticsearch.client.RestClient;
import org.apache.http.HttpHost;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.*;
import java.util.logging.Logger;
import java.lang.Math;

/**
 * RagTokenizer 的 Java 实现（参照你提供的 Python 代码）
 * Java 1.8 兼容
 */
public class RagTokenizer {
    private static final Logger logger = Logger.getLogger(RagTokenizer.class.getName());
    private  ESConnection esConn = new ESConnection(); // 依赖注入
    private boolean DEBUG = false;
    private int DENOMINATOR = 1000000;
    private String DIR_; // base dir + utils/res/huqie
    private String SPLIT_CHAR;
    private SimpleTrie trie_;
    public RagTokenizer() {
        this(false);
    }

    public RagTokenizer(boolean debug) {
        this.DEBUG = debug;
        this.DENOMINATOR = 1000000;
        this.DIR_ = getProjectBaseDirectory() + File.separator + "utils" + File.separator + "res" + File.separator + "huqie";
        // 与 Python 保持一致的 split 正则（注意 Java 字符串中需要双反斜杠）
        this.SPLIT_CHAR = "([ ,\\.<>/?;:'\\[\\]\\\\`!@#$%^&*\\(\\)\\{\\}\\|_+=《》，。？、；‘’：“”【】~！￥%……（）——-]+|[a-zA-Z0-9,\\.-]+)";

        String trieFileName = this.DIR_ + ".txt.trie";
        File f = new File(trieFileName);
        if (f.exists()) {
            try {
                this.trie_ = trie_.load(trieFileName);
                return;
            } catch (Exception ex) {
                logger.warning("[HUQIE]:Fail to load trie file " + trieFileName + ", build the default trie");
                this.trie_ = new SimpleTrie();
            }
        } else {
            logger.info("[HUQIE]:Trie file " + trieFileName + " not found, build the default trie file");
            this.trie_ = new SimpleTrie();
        }

        // 载入 dict 文件（DIR_ + ".txt"）
        this.loadDict_(this.DIR_ + ".txt");
    }

    // ---------- 辅助：返回项目根目录（和 Python get_project_base_directory 行为类似） ----------
    private String getProjectBaseDirectory() {
        try {
            // 取得当前类文件运行位置并上跳一级
            String path = new File(RagTokenizer.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            File dir = new File(path);
            File parent = dir.getParentFile();
            if (parent != null) return parent.getAbsolutePath();
            return dir.getAbsolutePath();
        } catch (Exception e) {
            return System.getProperty("user.dir");
        }
    }

    // key_：lower + utf-8 bytes string 表示风格（Python 的 str(...encode))，这里采用简单的 lower()UTF-8 bytes hex 表示以保证唯一
    private String key_(String line) {
        if (line == null) return null;
        try {
            String low = line.toLowerCase();
            byte[] bs = low.getBytes("UTF-8");
            // 用 hex 表示（类似 Python 的字节表示），但更简洁
            StringBuilder sb = new StringBuilder();
            for (byte b : bs) {
                int v = b & 0xFF;
                String hx = Integer.toHexString(v);
                if (hx.length() == 1) sb.append('0');
                sb.append(hx);
            }
            return sb.toString();
        } catch (Exception ex) {
            return line.toLowerCase();
        }
    }

    // rkey_: reversed + prefix "DD"
    private String rkey_(String line) {
        if (line == null) return null;
        String rev = new StringBuilder(line).reverse().toString();
        return key_("DD" + rev.toLowerCase());
    }

    /**
     * loadDict_：从一个 dict 文件读取并填充 trie_，并保存到 cache 文件 (.trie)
     * Python 里每行格式像： word <space> frequency_as_float <space> tag
     */
    private void loadDict_(String fnm) {
        logger.info("[HUQIE]:Build trie from " + fnm);
        try {
            java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream(fnm), "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.replaceAll("[\\r\\n]+", "");
                String[] parts = line.split("[ \\t]");
                if (parts.length < 2) continue;
                String k0 = parts[0];
                String k = this.key_(k0);
                double freqFloat = 1.0;
                try {
                    freqFloat = Double.parseDouble(parts[1]);
                } catch (Exception ex) {
                    // ignore parse error
                }
                int F = (int) (Math.log(freqFloat / (double) this.DENOMINATOR) + 0.5);
                String tag = parts.length >= 3 ? parts[2] : "";
                Object existing = this.trie_.get(k);
                if (existing == null || (existing instanceof Value && ((Value) existing).freqExp < F)) {
                    this.trie_.put(k, new Value(F, tag));
                }
                // rkey 存一个占位（Python 中是1）
                this.trie_.put(this.rkey_(k0), 1);
            }
            br.close();

            String dictFileCache = fnm + ".trie";
            logger.info("[HUQIE]:Build trie cache to " + dictFileCache);
            try {
                this.trie_.save(dictFileCache);
            } catch (Exception ex) {
                logger.warning("[HUQIE]:save trie cache fail: " + ex.getMessage());
            }
        } catch (Exception ex) {
            logger.severe("[HUQIE]:Build trie " + fnm + " failed: " + ex.getMessage());
        }
    }

    // external API：loadUserDict
    public void loadUserDict(String fnm) {
        try {
            this.trie_ = SimpleTrie.load(fnm + ".trie");
            return;
        } catch (Exception ex) {
            this.trie_ = new SimpleTrie();
        }
        this.loadDict_(fnm);
    }

    public void addUserDict(String fnm) {
        this.loadDict_(fnm);
    }

    // strQ2B: 全角转半角
    public String _strQ2B(String ustring) {
        if (ustring == null) return null;
        StringBuilder r = new StringBuilder();
        for (int i = 0; i < ustring.length(); i++) {
            char uchar = ustring.charAt(i);
            int inside_code = uchar;
            if (inside_code == 0x3000) {
                inside_code = 0x0020;
            } else {
                inside_code -= 0xfee0;
            }
            if (inside_code < 0x0020 || inside_code > 0x7e) {
                r.append(uchar);
            } else {
                r.append((char) inside_code);
            }
        }
        return r.toString();
    }

    // trad -> simp using opencc4j
    public String _tradi2simp(String line) {
        if (line == null) return null;
        try {
            return ZhConverterUtil.toSimple(line);
        } catch (Exception ex) {
            return line;
        }
    }

    // ---------- 深度优先分词搜索 dfs_（尽量保持 Python 行为） ----------
    /**
     * dfs_:
     * chars: char array of the token string (tk)
     * s: start index
     * preTks: list of Pair<String,Object> (token, valueFromTrie)
     * tkslist: output list of possible preTks sequences (List<List<Pair>>)
     */
    public int dfs_(char[] chars, int s, List<Pair<String, Object>> preTks, List<List<Pair<String, Object>>> tkslist) {
        return dfs_(chars, s, preTks, tkslist, 0, new HashMap<String, Integer>());
    }

    private int dfs_(char[] chars, int s, List<Pair<String, Object>> preTks, List<List<Pair<String, Object>>> tkslist, int _depth, Map<String, Integer> _memo) {
        if (_memo == null) _memo = new HashMap<String, Integer>();
        final int MAX_DEPTH = 10;
        if (_depth > MAX_DEPTH) {
            if (s < chars.length) {
                List<Pair<String, Object>> copyPretks = deepCopyPairs(preTks);
                String remaining = new String(chars, s, chars.length - s);
                copyPretks.add(new Pair<String, Object>(remaining, new Value(-12, "")));
                tkslist.add(copyPretks);
            }
            return s;
        }

        String stateKey;
        if (preTks != null && preTks.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (Pair<String, Object> p : preTks) {
                sb.append(p.getLeft());
                sb.append("|");
            }
            stateKey = s + ":" + sb.toString();
        } else {
            stateKey = s + ":#";
        }
        if (_memo.containsKey(stateKey)) {
            return _memo.get(stateKey);
        }

        int res = s;
        if (s >= chars.length) {
            tkslist.add(preTks);
            _memo.put(stateKey, s);
            return s;
        }
        if (s < chars.length - 4) {
            boolean isRepetitive = true;
            char charToCheck = chars[s];
            for (int i = 1; i < 5; i++) {
                if (s + i >= chars.length || chars[s + i] != charToCheck) {
                    isRepetitive = false;
                    break;
                }
            }
            if (isRepetitive) {
                int end = s;
                while (end < chars.length && chars[end] == charToCheck) end++;
                int mid = s + Math.min(10, end - s);
                String t = new String(chars, s, mid - s);
                String k = this.key_(t);
                List<Pair<String, Object>> copyPretks = deepCopyPairs(preTks);
                if (this.trie_.containsKey(k)) {
                    copyPretks.add(new Pair<String, Object>(t, this.trie_.get(k)));
                } else {
                    copyPretks.add(new Pair<String, Object>(t, new Value(-12, "")));
                }
                int nextRes = dfs_(chars, mid, copyPretks, tkslist, _depth + 1, _memo);
                res = Math.max(res, nextRes);
                _memo.put(stateKey, res);
                return res;
            }
        }

        int S = s + 1;
        if (s + 2 <= chars.length) {
            String t1 = new String(chars, s, 1);
            String t2 = new String(chars, s, 2);
            if (this.trie_.hasKeysWithPrefix(this.key_(t1)) && !this.trie_.hasKeysWithPrefix(this.key_(t2))) {
                S = s + 2;
            }
        }
        if (preTks.size() > 2 && preTks.get(preTks.size() - 1).getLeft().length() == 1
                && preTks.get(preTks.size() - 2).getLeft().length() == 1
                && preTks.get(preTks.size() - 3).getLeft().length() == 1) {
            String t1 = preTks.get(preTks.size() - 1).getLeft() + new String(chars, s, 1);
            if (this.trie_.hasKeysWithPrefix(this.key_(t1))) {
                S = s + 2;
            }
        }

        for (int e = S; e <= chars.length; e++) {
            String t = new String(chars, s, e - s);
            String k = this.key_(t);
            if (e > s + 1 && !this.trie_.hasKeysWithPrefix(k)) {
                break;
            }
            if (this.trie_.containsKey(k)) {
                List<Pair<String, Object>> pretks = deepCopyPairs(preTks);
                pretks.add(new Pair<String, Object>(t, this.trie_.get(k)));
                res = Math.max(res, dfs_(chars, e, pretks, tkslist, _depth + 1, _memo));
            }
        }

        if (res > s) {
            _memo.put(stateKey, res);
            return res;
        }

        String t = new String(chars, s, 1);
        String k = this.key_(t);
        List<Pair<String, Object>> copyPretks = deepCopyPairs(preTks);
        if (this.trie_.containsKey(k)) {
            copyPretks.add(new Pair<String, Object>(t, this.trie_.get(k)));
        } else {
            copyPretks.add(new Pair<String, Object>(t, new Value(-12, "")));
        }
        int result = dfs_(chars, s + 1, copyPretks, tkslist, _depth + 1, _memo);
        _memo.put(stateKey, result);
        return result;
    }

    // deep copy helpers for Pair lists
    private List<Pair<String, Object>> deepCopyPairs(List<Pair<String, Object>> src) {
        List<Pair<String, Object>> out = new ArrayList<Pair<String, Object>>();
        if (src == null) return out;
        for (Pair<String, Object> p : src) {
            out.add(new Pair<String, Object>(p.getLeft(), p.getRight()));
        }
        return out;
    }

    // score_ : 接受 List<Pair<String,Object>> 每个 Pair 的 right 期望为 Value (freqExp, tag)
    public Pair<List<String>, Double> score_(List<Pair<String, Object>> tfts) {
        double B = 30.0;
        double F = 0.0;
        double L = 0.0;
        List<String> tks = new ArrayList<String>();
        for (Pair<String, Object> p : tfts) {
            String tk = p.getLeft();
            Object val = p.getRight();
            int freq = 0;
            String tag = "";
            if (val instanceof Value) {
                freq = ((Value) val).freqExp;
                tag = ((Value) val).tag;
            } else if (val instanceof Integer) {
                freq = (Integer) val;
            }
            F += freq;
            if (tk.length() >= 2) L += 1.0;
            tks.add(tk);
        }
        if (tks.size() == 0) {
            return new Pair<List<String>, Double>(tks, 0.0);
        }
        L = L / tks.size();
        double score = B / tks.size() + L + F;
        if (this.DEBUG) {
            logger.info("[SC] " + tks.toString() + " " + tks.size() + " " + L + " " + F + " " + score);
        }
        return new Pair<List<String>, Double>(tks, score);
    }

    // sortTks_
    public List<Pair<List<String>, Double>> sortTks_(List<List<Pair<String, Object>>> tkslist) {
        List<Pair<List<String>, Double>> res = new ArrayList<Pair<List<String>, Double>>();
        for (List<Pair<String, Object>> tfts : tkslist) {
            Pair<List<String>, Double> p = score_(tfts);
            res.add(new Pair<List<String>, Double>(p.getLeft(), p.getRight()));
        }
        // sort by score desc
        Collections.sort(res, new Comparator<Pair<List<String>, Double>>() {
            public int compare(Pair<List<String>, Double> a, Pair<List<String>, Double> b) {
                return Double.compare(b.getRight(), a.getRight());
            }
        });
        return res;
    }

    // merge_ : 合并可能被 split char 切开的 token
    public String merge_(String tks) {
        if (tks == null) return null;
        tks = tks.replaceAll("[ ]+", " ");
        String[] parts = tks.trim().split("\\s+");
        List<String> res = new ArrayList<String>();
        int s = 0;
        while (true) {
            if (s >= parts.length) break;
            int E = s + 1;
            for (int e = s + 2; e < Math.min(parts.length + 2, s + 6); e++) {
                String tk = "";
                for (int i = s; i < e && i < parts.length; i++) tk += parts[i];
                if (Pattern.compile(this.SPLIT_CHAR).matcher(tk).find() && this.freq(tk) > 0) {
                    E = e;
                }
            }
            StringBuilder sb = new StringBuilder();
            for (int i = s; i < E && i < parts.length; i++) sb.append(parts[i]);
            res.add(sb.toString());
            s = E;
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < res.size(); i++) {
            if (i > 0) out.append(" ");
            out.append(res.get(i));
        }
        return out.toString();
    }

    // maxForward_
    public Pair<List<String>, Double> maxForward_(String line) {
        List<Pair<String, Object>> res = new ArrayList<Pair<String, Object>>();
        int s = 0;
        while (s < line.length()) {
            int e = s + 1;
            String t = line.substring(s, e);
            while (e < line.length() && this.trie_.hasKeysWithPrefix(this.key_(t))) {
                e += 1;
                t = line.substring(s, e);
            }
            while (e - 1 > s && !this.trie_.containsKey(this.key_(t))) {
                e -= 1;
                t = line.substring(s, e);
            }
            if (this.trie_.containsKey(this.key_(t))) {
                res.add(new Pair<String, Object>(t, this.trie_.get(this.key_(t))));
            } else {
                res.add(new Pair<String, Object>(t, 0));
            }
            s = e;
        }
        return score_(res);
    }

    // maxBackward_
    public Pair<List<String>, Double> maxBackward_(String line) {
        List<Pair<String, Object>> res = new ArrayList<Pair<String, Object>>();
        int s = line.length() - 1;
        while (s >= 0) {
            int e = s + 1;
            String t = line.substring(s, e);
            while (s > 0 && this.trie_.hasKeysWithPrefix(this.rkey_(t))) {
                s -= 1;
                t = line.substring(s, e);
            }
            while (s + 1 < e && !this.trie_.containsKey(this.key_(t))) {
                s += 1;
                t = line.substring(s, e);
            }
            if (this.trie_.containsKey(this.key_(t))) {
                res.add(new Pair<String, Object>(t, this.trie_.get(this.key_(t))));
            } else {
                res.add(new Pair<String, Object>(t, 0));
            }
            s -= 1;
        }
        // reverse order then score_
        List<Pair<String, Object>> rev = new ArrayList<Pair<String, Object>>();
        for (int i = res.size() - 1; i >= 0; i--) rev.add(res.get(i));
        return score_(rev);
    }

    // _split_by_lang: 返回 List<Pair<String,Boolean>> 表示 text + isChinese
    public List<Pair<String, Boolean>> _split_by_lang(String line) {
        List<Pair<String, Boolean>> out = new ArrayList<Pair<String, Boolean>>();
        String[] arr = line.split(this.SPLIT_CHAR);
        for (String a : arr) {
            if (a == null || a.length() == 0) continue;
            int s = 0;
            int e = s + 1;
            boolean zh = is_chinese(a.charAt(s));
            while (e < a.length()) {
                boolean _zh = is_chinese(a.charAt(e));
                if (_zh == zh) {
                    e += 1;
                    continue;
                }
                out.add(new Pair<String, Boolean>(a.substring(s, e), zh));
                s = e;
                e = s + 1;
                zh = _zh;
            }
            if (s >= a.length()) continue;
            out.add(new Pair<String, Boolean>(a.substring(s, e), zh));
        }
        return out;
    }

    // tokenize: 使用 ES _analyze（ik_smart），并返回 merge_ 后的结果
    public  String tokenize(String line) {
        // 预编译正则，线程安全可复用
        Pattern p = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        String clean_line = p.matcher(line).replaceAll(" ").trim();
        // 1. 替换非单词字符为空格
//        line = line.replaceAll("\\W+", " ");

        // 2. 调用你已有的工具方法
        line = this._strQ2B(clean_line).toLowerCase();
        line = this._tradi2simp(line);

//        // 3. 连接 Elasticsearch
//        String hostname = "10.3.24.46";
//        int port = 9200;
//        String username = "elastic";
//        String password = "sciyon";
//
//        RestClient restClient = RestClient.builder(new HttpHost(hostname, port, "http"))
//                .setDefaultHeaders(new org.apache.http.Header[]{
//                        new org.apache.http.message.BasicHeader("Authorization",
//                                "Basic " + java.util.Base64.getEncoder().encodeToString((username + ":" + password).getBytes()))
//                })
//                .build();
//
//        ElasticsearchClient client = new ElasticsearchClient(
//                new RestClientTransport(restClient, new JacksonJsonpMapper())
//        );

        List<String> tokens = new ArrayList<>();
        final String textForAnalyze = line;

        try {
            // 4. 调用 _analyze API
            AnalyzeRequest req = AnalyzeRequest.of(a -> a
                    .analyzer("ik_smart")  // 也可以改为 ik_max_word
                    .text(textForAnalyze)
            );

//            AnalyzeResponse resp = client.indices().analyze(req);
            AnalyzeResponse resp = this.esConn.getClient().indices().analyze(req);

            if (resp.tokens() != null) {
                for (AnalyzeToken t : resp.tokens()) {
                    tokens.add(t.token());
                }
            }

        } catch (IOException e) {
            System.out.println("调用 Elasticsearch _analyze API 时发生错误:");
            e.printStackTrace();
        }

        // 5. 拼接分词结果
        String res = String.join(" ", tokens);
        logger.fine("[TKS] " + this.merge_(res));
        return this.merge_(res);
    }
    // fine_grained_tokenize
    public String fine_grained_tokenize(String tks) {
        if (tks == null) return null;
        String[] arr = tks.split("\\s+");
        List<String> tksList = new ArrayList<String>(Arrays.asList(arr));
        int zh_num = 0;
        for (String c : tksList) {
            if (c != null && c.length() > 0 && is_chinese(c.charAt(0))) zh_num++;
        }
        if (zh_num < tksList.size() * 0.2) {
            List<String> res = new ArrayList<String>();
            for (String tk : tksList) {
                String[] pieces = tk.split("/");
                for (String p : pieces) {
                    if (p.length() > 0) res.add(p);
                }
            }
            return String.join(" ", res);
        }

        List<String> res = new ArrayList<String>();
        for (String tk : tksList) {
            if (tk.length() < 3 || tk.matches("[0-9,\\.-]+$")) {
                res.add(tk);
                continue;
            }
            List<List<Pair<String, Object>>> tkslist = new ArrayList<List<Pair<String, Object>>>();
            if (tk.length() > 10) {
                List<Pair<String, Object>> one = new ArrayList<Pair<String, Object>>();
                one.add(new Pair<String, Object>(tk, null));
                tkslist.add(one);
            } else {
                this.dfs_(tk.toCharArray(), 0, new ArrayList<Pair<String, Object>>(), tkslist);
            }
            if (tkslist.size() < 2) {
                res.add(tk);
                continue;
            }
            List<Pair<List<String>, Double>> sorted = this.sortTks_(tkslist);
            // Python 中取第二项 sorted(...)[1][0]，注意越界检查
            List<String> stk = null;
            if (sorted.size() > 1) {
                stk = sorted.get(1).getLeft();
            } else {
                stk = sorted.get(0).getLeft();
            }
            String joined;
            if (stk.size() == tk.length()) {
                joined = tk;
            } else {
                if (tk.matches("[a-z\\.-]+$")) {
                    boolean shortFound = false;
                    for (String t : stk) {
                        if (t.length() < 3) {
                            shortFound = true;
                            break;
                        }
                    }
                    if (shortFound) {
                        joined = tk;
                    } else {
                        joined = String.join(" ", stk);
                    }
                } else {
                    joined = String.join(" ", stk);
                }
            }
            res.add(joined);
        }
        return String.join(" ", res);
    }

    // freq: 从 trie 中取值
    public double freq(String tk) {
        String k = this.key_(tk);
        if (!this.trie_.containsKey(k)) return 0;
        Object val = this.trie_.get(k);
        if (val instanceof Value) {
            double v = Math.exp(((Value) val).freqExp) * this.DENOMINATOR + 0.5;
            return (int) v;
        } else if (val instanceof Integer) {
            return (Integer) val;
        } else {
            return 0;
        }
    }

    // tag
    public String tag(String tk) {
        String k = this.key_(tk);
        if (!this.trie_.containsKey(k)) return "";
        Object val = this.trie_.get(k);
        if (val instanceof Value) {
            return ((Value) val).tag;
        }
        return "";
    }

    // helper: is Chinese char
    public static boolean is_chinese(char s) {
        return s >= '\u4e00' && s <= '\u9fa5';
    }

    // ----- 内部静态类：Value，Pair （简单泛型 Pair） -----
    public static class Value implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int freqExp;
        public String tag;

        public Value(int f, String t) {
            this.freqExp = f;
            this.tag = t;
        }
    }

    // 简单 Pair 类（左、右）
    public static class Pair<L, R> {
        private L left;
        private R right;

        public Pair(L l, R r) {
            this.left = l;
            this.right = r;
        }

        public L getLeft() { return left; }
        public R getRight() { return right; }
        public void setLeft(L l) { this.left = l; }
        public void setRight(R r) { this.right = r; }
    }
}
