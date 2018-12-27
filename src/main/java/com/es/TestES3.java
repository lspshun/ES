package com.es;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Description：elasticSearch高级查询演示<br/>
 */
public class TestES3 {
    private final static String INDEX = "mytest";
    private final static String TYPE = "product";
    //存储索引库的容器
    private final static String[] indices = {INDEX};
    private TransportClient client;

    /**
     * 执行初始化
     */
    @Before
    public void init() throws UnknownHostException {
        // 设置连接集群的名称
        Settings settings = Settings.builder().put("cluster.name", "bigdata").build();
        // 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("bigdata"), 9300));
        // 打印集群名称
        System.out.println(client.toString());
    }

    /**
     * 测试分页查询以及查询方式
     * <p>
     * 查询方式有：
     * SearchType:
     * query and fetch(速度最快)(返回N倍数据量,高版本的es已经不推荐使用了)
     * query then fetch（默认的搜索方式）
     * DFS query and fetch  ~>用来给检索到的每条记录计算分数
     * DFS query then fetch(可以更精确控制搜索打分和排名。)
     * <p>
     * 案例1：检索bigdata索引库中，product type中的字段name为kafka的索引信息。
     * 学习知识点： 检索类型，分页检索
     */
    @Test
    public void testSearchTypeAndSplitSearch() {
        SearchResponse response = client.prepareSearch(indices)
                //设置分页
                .setFrom(0)
                .setSize(2)
                //设置查询条件
                .setQuery(QueryBuilders.matchQuery("name", "storm"))
                //设置检索类型
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                //.setSearchType(SearchType.QUERY_AND_FETCH)
                //触发执行
                .get();

        //分析反馈的结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

    }

    /**
     * 案例2： 检索bigdata索引库中，product type中的字段name中包含hive的索引信息，需要高亮显示。
     * <p>
     * 最终希望的结果是：
     * {"name":"Apache <font color='red' size='20'>Hive</font>","author":"刘德华","version":"2.5.1"}
     */
    @Test
    public void testHightLightShow() {
        //准备高亮显示构建器HighlightBuilder的实例,
        HighlightBuilder builder = new HighlightBuilder();
        builder.preTags("<font color='red' size='20'>")
                .postTags("</font>")
                .field("name");

        SearchResponse response = client.prepareSearch(indices)
                //设置查询条件
                .setQuery(QueryBuilders.fuzzyQuery("name", "storm"))
                //设置高亮显示
                .highlighter(builder)
                //触发执行
                .get();

        //分析反馈的结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //map封装了检索到的一条记录
            Map<String, Object> map = hit.getSourceAsMap();

            //分析索引信息,将检索关键字对应的key的值进行替换
            //下述的Map容器封装了需要高亮显示的字段，key对应的值就包含了前后缀
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            for (Map.Entry<String, HighlightField> entry : highlightFields.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().getFragments()[0].toString();
                map.put(key, value);
            }

            //打印输出当前的索引信息
            System.out.println(JSON.toJSONString(map));
        }
    }

    /**
     * 聚合查询演示
     * <p>
     * 需求：查询bank索引库中，所有女性员工数，最年轻的员工的年龄，最低薪水，以及平均薪水。
     * select  count(*) cnt , min(age) age, min(salary) salary ,avg(salary) avgSalary from account where gender='F'
     */
    @Test
    public void testAggregationSearch() {
        SearchResponse response = client.prepareSearch("bank")
                .setTypes("account")
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("gender.keyword", "F")))
                //.addAggregation(new ValueCountAggregationBuilder("cnt", ValueType.LONG).field("firstname"))
                .addAggregation(new MinAggregationBuilder("minAge").field("age"))
                .addAggregation(new MinAggregationBuilder("minSalay").field("balance"))
                .addAggregation(new AvgAggregationBuilder("avgSalary").field("balance"))
                .get();

        //分析反馈的结果
        Aggregations aggregations = response.getAggregations();

        //ValueCountAggregator cnt = aggregations.get("cnt");
        Min minAge = aggregations.get("minAge");
        Min minSalay = aggregations.get("minSalay");
        Avg avgSalary = aggregations.get("avgSalary");

        //System.out.printf("总人数：%s, 最小的女性员工的年龄：%f，最低薪资：%f,平均薪资：%.2f%n", cnt, minAge, minSalay, avgSalary);
        System.out.printf(" 最小的女性员工的年龄：%s，最低薪资：%s,平均薪资：%.2f%n", minAge.getValue(), minSalay.getValue(), avgSalary.getValue());
    }


    @After
    public void cleanUp() {
        //System.out.println("资源释放哦。。。");
        if (client != null) {
            client.close();
        }
    }
}
