package TokenizerConvert;

import java.util.Arrays;
import java.util.List;

public class Main {

//    public static void main(String[] args) {
//        // 1. 创建 RagTokenizer 实例
//        RagTokenizer tokenizer = new RagTokenizer();
//
//        // 2. 定义测试字符串
//        String str = "2.2模型提供商配置\n添加嵌入模型（固定配置，切勿随意修改）";
//
//        // 3. 调用 tokenize 方法
//        String result = tokenizer.tokenize(str);
//
//        // 4. 打印结果
//        System.out.println("分词结果: " + result);
//    }

    public static void main(String[] args) throws Exception {
        // 1. 创建 Es 实例
        ESConnection es = new ESConnection();

        //2. 初始化实例
        Dealer  init_search_module = new Dealer(es);

        //3. 定义测试字符串
        String question = "引言\n偶像一\n\n1.首先是张婧仪";
        Object embdMdl = new Object();
        List<String> kbIds= Arrays.asList("571776832787972098");
        int page = 1;
        int size = 10;
        double similarityThreshold = 0.2;
        double vectorSimilarityWeight = 0.3;
        int top = 10;
        List<String> docIds= Arrays.asList("673484243964461057");
        boolean aggs =false ;
        Object rerankMdl = new Object();
        boolean highlight = false;
        // 4. 调用 tokenize 方法
        String result = init_search_module.retriever(question,embdMdl, kbIds,page,size,similarityThreshold, vectorSimilarityWeight,top,docIds,aggs,rerankMdl,highlight).toString();

        // 5. 打印结果`
        System.out.println("分词结果: " + result);
    }
}