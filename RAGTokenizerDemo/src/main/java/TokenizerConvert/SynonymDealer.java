package TokenizerConvert;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import net.sf.extjwnl.dictionary.Dictionary;
import net.sf.extjwnl.data.Synset;
import net.sf.extjwnl.data.Word;
import net.sf.extjwnl.data.IndexWord;
import net.sf.extjwnl.data.POS;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.util.*;

public class SynonymDealer {

    private static final Logger logger = LoggerFactory.getLogger(SynonymDealer.class);

    private long lookupNum;
    private long loadTm;
    private Map<String, Object> dictionary;
    private Object redis; // 占位，可以替换为 Jedis 或 Lettuce
    private Dictionary wordnet;

    public SynonymDealer() {
        this.lookupNum = 100000000L;
        this.loadTm = System.currentTimeMillis() / 1000 - 1000000L;
        this.dictionary = new HashMap<>();

        String path = Paths.get(getProjectBaseDirectory(), "utils", "res", "synonym.json").toString();
        try (Reader reader = new InputStreamReader(new FileInputStream(path), "UTF-8")) {
            Gson gson = new Gson();
            Type type = new TypeToken<Map<String, Object>>() {}.getType();
            dictionary = gson.fromJson(reader, type);
        } catch (Exception e) {
            logger.warn("Missing synonym.json");
            dictionary = new HashMap<>();
        }

        if (redis == null) {
            logger.warn("Realtime synonym is disabled, since no redis connection.");
        }
        if (dictionary.isEmpty()) {
            logger.warn("Fail to load synonym");
        }

        this.redis = redis;

        try {
            wordnet = Dictionary.getDefaultResourceInstance();
        } catch (Exception e) {
            logger.error("Failed to initialize WordNet: " + e.getMessage());
        }

        load();
    }

    private static String getProjectBaseDirectory() {
        File current = new File(SynonymDealer.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        return current.getParentFile().getParentFile().getAbsolutePath();
    }

    public void load() {
        if (redis == null) return;
        if (lookupNum < 100) return;

        long tm = System.currentTimeMillis() / 1000;
        if (tm - loadTm < 3600) return;

        loadTm = tm;
        lookupNum = 0;

        // TODO: 根据你的 Redis 客户端来实现
        // String d = redis.get("kevin_synonyms");
        String d = null; // 这里只是占位
        if (d == null) return;

        try {
            Gson gson = new Gson();
            Type type = new TypeToken<Map<String, Object>>() {}.getType();
            dictionary = gson.fromJson(d, type);
        } catch (Exception e) {
            logger.error("Fail to load synonym! " + e.getMessage());
        }
    }

    public List<String> lookup(String tk, int topn) {
        if (tk == null || tk.isEmpty()) {
            return Collections.emptyList();
        }

        // 英文字母单词走 WordNet 分支
        if (tk.matches("[a-z]+$")) {
            Set<String> res = new LinkedHashSet<>(); // 去重且保留顺序
            try {
                POS[] poses = new POS[]{ POS.NOUN, POS.VERB, POS.ADJECTIVE, POS.ADVERB };
                for (POS pos : poses) {
                    IndexWord iw = null;
                    try {
                        iw = wordnet.getIndexWord(pos, tk);
                    } catch (Exception e) {
                        // 单个 POS 查询失败继续下一个 POS
                        logger.debug("getIndexWord failed for pos " + pos + ": " + e.getMessage());
                    }
                    if (iw == null) continue;
                    List<Synset> senses = iw.getSenses();
                    if (senses == null) continue;
                    for (Synset sense : senses) {
                        List<Word> words = sense.getWords();
                        if (words == null) continue;
                        for (Word w : words) {
                            String name = w.getLemma();
                            if (name == null) continue;
                            name = name.replace('_', ' ');
                            if (!name.equalsIgnoreCase(tk)) {
                                res.add(name);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("WordNet lookup failed: " + e.getMessage(), e);
            }
            List<String> list = new ArrayList<>(res);
            return list.size() > topn ? list.subList(0, topn) : list;
        }

        // 非英文分支：使用本地 dictionary（注意：这里用 Java 的 replaceAll 替代 Python 的 re.sub）
        this.lookupNum += 1;
        this.load();

        String key = tk.toLowerCase().replaceAll("[ \\t]+", " ");
        Object obj = this.dictionary.get(key);

        List<String> resList = new ArrayList<>();
        if (obj == null) {
            // nothing
        } else if (obj instanceof String) {
            resList.add((String) obj);
        } else if (obj instanceof List) {
            for (Object o : (List<?>) obj) {
                if (o != null) resList.add(o.toString());
            }
        } else if (obj instanceof String[]) {
            for (String s : (String[]) obj) {
                if (s != null) resList.add(s);
            }
        } else {
            // 其他类型也尝试用 toString 作为兜底
            resList.add(obj.toString());
        }

        return resList.size() > topn ? resList.subList(0, topn) : resList;
    }

//    public static void main(String[] args) {
//        SynonymDealer dl = new SynonymDealer(null);
//        System.out.println(dl.dictionary);
//    }
}
