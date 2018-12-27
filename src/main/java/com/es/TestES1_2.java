package com.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.entry.Product;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: lsp
 * @Date: 2018/12/25 20:10
 * @Description:常用的CRUD操作
 */
public class TestES1_2 {
    private final String INDEX = "mytest";
    private final String TYPE = "Product";
    private TransportClient client;

    /**
     * @description 执行初始化
     **/
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

    /*
     * 添加索引的四中方式：
     * 1、json字符串
     * 2、对象
     * 3、map
     * 4、XContentBuilder
     **/
    @Test  // 测试新增索引
    public void testInsert() {
        String source = JSON.toJSONString(new Product("storm", "张学友", "3.45"));
        System.out.println(source);
        IndexResponse response = client.prepareIndex(INDEX, TYPE, "1").setSource(source, XContentType.JSON).get();
        System.out.println(response);
    }
    @Test
    public void testInsertJSON() {
        String source = "{\"name\":\"sqoop\", \"author\":\"Apache\", \"version\":\"1.4.7\"}";
        IndexResponse response = client.prepareIndex("product", "bigdata", "3").setSource(source)/*.execute().actionGet()*/.get();
        System.out.println("insert json:" + response.getVersion());
    }

    @Test
    public void testInsertObj() {
        Product product = new Product("kafka", "LinkIn", "0.10.0.1");
        JSONObject jsonObj = new JSONObject((Map<String, Object>) product);
        System.out.println(jsonObj.toString());
        IndexResponse response = client.prepareIndex("product", "bigdata", "6")
                .setSource(jsonObj.toString())
                .get();
//        IndexResponse response = client.prepareIndex("product", "bigdata", "5")
//                .setSource("name", "es", "author", "oldli")
//                .get();
        System.out.println("insert obj:" + response.getVersion());
    }
    private String index = "product";
    private String type = "bigdata";
    @Test
    public void testInsertMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("p", "张三");
        map.put("2p", "0.1.1");
        map.put("4p", "王帆");
        IndexResponse response = client.prepareIndex(index, type, "7")
                .setSource(map).get();
        System.out.println("insert map:" + response.getVersion());
    }

    @Test
    public void testInsertXContentBuilder() throws IOException {
        XContentBuilder xContent = JsonXContent.contentBuilder();
        xContent.startObject()
                .field("name", "storm")
                .field("version", "1.0.2")
                .field("author", "twitter")
                .endObject();
        IndexResponse response = client.prepareIndex(index, type, "8")
                .setSource(xContent).get();
        System.out.println("insert map:" + response.getVersion());
    }

    @Test  // 测试删除索引
    public void testDelet() {
        DeleteResponse response = client.prepareDelete(INDEX, TYPE, "3").get();
        String result = response.getResult().getLowercase();
        //占位符：%s ~>字符串； %d ~> 整数； %.2f~>小数(指定小数点后精确的位数)
        //%n： 换行
        System.out.printf("删除成功否？%s%n", "deleted".equals(result) ? "恭喜！成功！..." : "抱歉！删除失败！555。。。");
    }

    @Test  //测试更新  将索引标识为3的索引信息author更新为jack version更新为666  局部更新
    public void testUpdate() {
        //UpdateResponse response = client.prepareUpdate(INDEX, TYPE, "3").setDoc(JSON.toJSONString(new Product("jack", "666")), XContentType.JSON).get();
        UpdateResponse response = client.prepareUpdate(INDEX, TYPE, "3").setDoc(JSON.toJSONString(new Product("Apache Hive")), XContentType.JSON).get();
        System.out.println("反馈的结果是：" + response);
    }

    @Test  //测试查询  需求：查询索引标识值为2的索引信息
    public void testGet() {
        GetResponse response = client.prepareGet(INDEX, TYPE, "3").get();
        System.out.println("检索到的索引信息是：" + response.getSourceAsString());
    }

    @Test  //elasticSearch批处理操作演示
    public void testBulk() {
        BulkResponse bulkResponse = client.prepareBulk()
                .add(new IndexRequest(INDEX, TYPE).source(JSON.toJSONString(new Product("kafka", "陆小凤", "3.6.8")), XContentType.JSON))
                .add(new UpdateRequest(INDEX, TYPE, "3").doc("name", "Flume"))
                .add(new DeleteRequest(INDEX, TYPE, "1"))
                .get();

        BulkItemResponse[] items = bulkResponse.getItems();
        for (BulkItemResponse item : items) {
            // item.isFailed() ~>删除时，若没有发生异常，都算成功。（排除情形：索引不存在，删除不了）
            System.out.println(item.isFailed() ? "失败" : "成功");//true:失败；fasle:成功
        }
    }

    @After  //运行结束释放资源
    public void cleanUp() {
        if (client != null) {
            client.close();
        }
    }
}
