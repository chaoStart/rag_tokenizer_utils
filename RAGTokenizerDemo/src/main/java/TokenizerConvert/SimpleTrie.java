// SimpleTrie.java
package TokenizerConvert;

import java.io.*;
import java.util.*;

/**
 * 一个轻量的 Trie 实现（支持序列化到文件与从文件加载）。
 * - key 存储为 String（已按 Python 原始 key_ 生成后的形式）
 * - value 存储为 Object（通常我们放 Value 或 Integer）
 */
public class SimpleTrie implements Serializable {
    private static final long serialVersionUID = 1L;

    // 直接用 map 存 key->value，hasKeysWithPrefix 通过遍历 keys 实现（简单实现，便于替换）
    private Map<String, Object> map;

    public SimpleTrie() {
        this.map = new HashMap<String, Object>();
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public Object get(String key) {
        return map.get(key);
    }

    public void put(String key, Object value) {
        map.put(key, value);
    }

    /**
     * 判断是否存在以 prefix 开头的 key
     */
    public boolean hasKeysWithPrefix(String prefix) {
        if (prefix == null || prefix.length() == 0) return false;
        for (String k : map.keySet()) {
            if (k.startsWith(prefix)) return true;
        }
        return false;
    }

    /**
     * 保存到文件（序列化）
     */
    public void save(String filePath) throws IOException {
        FileOutputStream fos = new FileOutputStream(filePath);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(this.map);
        oos.close();
        fos.close();
    }

    /**
     * 从文件加载（反序列化）
     */
    @SuppressWarnings("unchecked")
    public static SimpleTrie load(String filePath) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(filePath);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object obj = ois.readObject();
        ois.close();
        fis.close();
        SimpleTrie t = new SimpleTrie();
        if (obj instanceof Map) {
            t.map = (Map<String, Object>) obj;
        }
        return t;
    }

    /**
     * 返回内部 map（谨慎使用）
     */
    public Map<String, Object> getMap() {
        return this.map;
    }
}
