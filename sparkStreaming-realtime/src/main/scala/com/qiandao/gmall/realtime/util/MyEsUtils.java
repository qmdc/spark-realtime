package com.qiandao.gmall.realtime.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.qiandao.gmall.realtime.bean.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MyEsUtils {

    private static ElasticsearchClient esClient = create();

    public static ElasticsearchClient create() {
        RestClient restClient = RestClient.builder(new HttpHost("hadoop", 9200)).build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    public static void bulkSaveDauInfo(String indexName, List<Tuple2<String, DauInfo>> docs /*在java中使用scala的元组*/) {
        BulkRequest.Builder builder = new BulkRequest.Builder();
        for (Tuple2<String, DauInfo> doc : docs) {
            //这里本考虑用json转换，但是fastjson转换后一直为null
            DauInfoJava dauInfoJava = new DauInfoJava();

            dauInfoJava.setMid(doc._2.mid());
            dauInfoJava.setUser_age(doc._2.user_id());
            dauInfoJava.setProvince_id(doc._2.province_id());
            dauInfoJava.setChannel(doc._2.channel());
            dauInfoJava.setIs_new(doc._2.is_new());
            dauInfoJava.setModel(doc._2.model());
            dauInfoJava.setOperate_system(doc._2.operate_system());
            dauInfoJava.setVersion_code(doc._2.version_code());
            dauInfoJava.setPage_id(doc._2.page_id());
            dauInfoJava.setPage_item(doc._2.page_item());
            dauInfoJava.setPage_item_type(doc._2.page_item_type());
            dauInfoJava.setSourceType(doc._2.sourceType());
            dauInfoJava.setDuring_time(doc._2.during_time());

            dauInfoJava.setUser_gender(doc._2.user_gender());
            dauInfoJava.setUser_age(doc._2.user_age());

            dauInfoJava.setProvince_name(doc._2.province_name());
            dauInfoJava.setProvince_iso_code(doc._2.province_iso_code());
            dauInfoJava.setProvince_3166_2(doc._2.province_3166_2());
            dauInfoJava.setProvince_area_code(doc._2.province_area_code());

            dauInfoJava.setDt(doc._2.dt());
            dauInfoJava.setHr(doc._2.hr());
            dauInfoJava.setTs(doc._2.ts());

            builder.operations(op -> op
                    .index(idx -> idx
                            .index(indexName)
                            .id(doc._1)
                            .document(dauInfoJava)));
        }
        try {
            esClient.bulk(builder.build());
        } catch (IOException e) {
            System.err.println("es保存有异常！");
        }
    }

    public static void bulkSaveOrder(String indexName, List<Tuple2<String, OrderWide>> docs /*在java中使用scala的元组*/) {
        BulkRequest.Builder builder = new BulkRequest.Builder();
        for (Tuple2<String, OrderWide> doc : docs) {
            OrderWideJava orderWideJava = new OrderWideJava();

            orderWideJava.setDetail_id(doc._2.detail_id());
            orderWideJava.setOrder_id(doc._2.order_id());
            orderWideJava.setSku_id(doc._2.sku_id());
            orderWideJava.setOrder_price(doc._2.order_price());
            orderWideJava.setSku_num(doc._2.sku_num());
            orderWideJava.setSku_name(doc._2.sku_name());
            orderWideJava.setSplit_total_amount(doc._2.split_total_amount());
            orderWideJava.setSplit_activity_amount(doc._2.split_activity_amount());
            orderWideJava.setSplit_coupon_amount(doc._2.split_coupon_amount());

            orderWideJava.setProvince_id(doc._2.province_id());
            orderWideJava.setOrder_status(doc._2.order_status());
            orderWideJava.setUser_id(doc._2.user_id());
            orderWideJava.setTotal_amount(doc._2.total_amount());
            orderWideJava.setActivity_reduce_amount(doc._2.activity_reduce_amount());
            orderWideJava.setCoupon_reduce_amount(doc._2.coupon_reduce_amount());
            orderWideJava.setOriginal_total_amount(doc._2.original_total_amount());
            orderWideJava.setFeight_fee(doc._2.feight_fee());
            orderWideJava.setFeight_fee_reduce(doc._2.feight_fee_reduce());
            orderWideJava.setExpire_time(doc._2.expire_time());
            orderWideJava.setRefundable_time(doc._2.refundable_time());
            orderWideJava.setCreate_time(doc._2.create_time());
            orderWideJava.setOperate_time(doc._2.operate_time());
            orderWideJava.setCreate_date(doc._2.create_date());
            orderWideJava.setCreate_hour(doc._2.create_hour());

            orderWideJava.setProvince_name(doc._2.province_name());
            orderWideJava.setProvince_area_code(doc._2.province_area_code());
            orderWideJava.setProvince_3166_2_code(doc._2.province_3166_2_code());
            orderWideJava.setProvince_iso_code(doc._2.province_iso_code());

            orderWideJava.setUser_age(doc._2.user_age());
            orderWideJava.setUser_gender(doc._2.user_gender());

            System.out.println("转换后：=>"+orderWideJava.toString());

            builder.operations(op -> op
                    .index(idx -> idx
                            .index(indexName)
                            .id(doc._1)
                            .document(orderWideJava)));
        }
        try {
            esClient.bulk(builder.build());
        } catch (IOException e) {
            System.err.println("es保存有异常！");
        }
    }

    public static List<String> searchField(String indexName, String fieldName){
        //判断索引是否存在
        boolean flag = false;
        try {
            flag = esClient.indices().exists(b -> b.index(indexName)).value();
        } catch (IOException e) {
            System.out.println("异常！");
        }
        if(!flag){
            return null;
        }

        SearchResponse<Map> response = null;
        try {
            response = esClient.search(s -> s
                            .index(indexName)
                            .size(1000)
                            .source(_s -> _s
                                    .filter(_f -> _f
                                            .includes(fieldName)))
                    , Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("查询结果:"+response);

        ArrayList<String> mids = new ArrayList<>();

        if (response != null) {
            List<Hit<Map>> hits = response.hits().hits();
            for (Hit<Map> hit : hits) {
                Map map = hit.source();
                if (map != null) {
                    String mid = map.get(fieldName).toString();
                    mids.add(mid);
                }
            }
        }

        return mids;
    }

}
