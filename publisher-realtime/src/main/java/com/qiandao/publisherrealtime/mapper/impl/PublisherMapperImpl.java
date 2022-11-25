package com.qiandao.publisherrealtime.mapper.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import com.qiandao.publisherrealtime.bean.NameValue;
import com.qiandao.publisherrealtime.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

    @Autowired
    private ElasticsearchClient esClient;

    private final String dauIndexNamePrefix = "gmall_dau_info_";
    private final String orderIndexNamePrefix = "gmall_order_wide_";


    @Override
    public List<NameValue> searchStatsByItemGender(String itemName, String date, String typeToField) {
        ArrayList<NameValue> results = new ArrayList<>();
        String indexName = orderIndexNamePrefix + date;
        SearchResponse<NameValue> search = null;
        try {
            search = esClient.search(s -> s
                            .index(indexName)
                            .query(q -> q
                                    .match(m -> m
                                            .field("sku_name")
                                            .query(itemName)
                                            .operator(Operator.And)))
                            .aggregations("groupByGender", a1 -> a1
                                    .terms(t -> t
                                            .field(typeToField)
                                            .size(100))
                                    .aggregations("totalAmount", a2 -> a2
                                            .sum(sum -> sum
                                                    .field("split_total_amount"))))
                            .size(0)
                    , NameValue.class);
        } catch (IOException | ElasticsearchException e) {
            log.info("查询交易分析业务异常。。");
        }
        if (search != null) {
            List<StringTermsBucket> groupByGender = search.aggregations().get("groupByGender").sterms().buckets().array();
            for (StringTermsBucket bucket : groupByGender) {
                String key = bucket.key();
                double totalAmount = bucket.aggregations().get("totalAmount").sum().value();
                results.add(new NameValue(key, totalAmount));
            }
        }
        log.info("交易分析业务：${}", results);
        return results;
    }

    @Override
    public List<NameValue> searchStatsByItemAge(String itemName, String date, String typeToField) {
        ArrayList<NameValue> results = new ArrayList<>();
        String indexName = orderIndexNamePrefix + date;
        SearchResponse<NameValue> search = null;
        try {
            search = esClient.search(s -> s
                            .index(indexName)
                            .query(q -> q
                                    .match(m -> m
                                            .field("sku_name")
                                            .query(itemName)
                                            .operator(Operator.And)))
                            .aggregations("groupByGender", a1 -> a1
                                    .terms(t -> t
                                            .field(typeToField)
                                            .size(100))
                                    .aggregations("totalAmount", a2 -> a2
                                            .sum(sum -> sum
                                                    .field("split_total_amount"))))
                            .size(0)
                    , NameValue.class);
        } catch (IOException | ElasticsearchException e) {
            log.info("查询交易分析业务异常。。");
        }
        if (search != null) {
            List<LongTermsBucket> groupByGender = search.aggregations().get("groupByGender").lterms().buckets().array();
            for (LongTermsBucket bucket : groupByGender) {
                Long key = bucket.key();
                double totalAmount = bucket.aggregations().get("totalAmount").sum().value();
                results.add(new NameValue(key, totalAmount));
            }
        }
        log.info("交易分析业务：${}", results);
        return results;
    }

    @Override
    public Map<String, Object> searchDetailByItem(String date, String itemName, int from, Integer pageSize) {
        HashMap<String, Object> results = new HashMap<>();
        SearchResponse<HashMap> search = null;
        try {
            search = esClient.search(s -> s
                            .index(orderIndexNamePrefix + date)
                            .query(q -> q
                                    .match(m -> m
                                            .field("sku_name")
                                            .query(itemName)
                                            .operator(Operator.And)))
                            .from(from)
                            .size(pageSize)
                            .highlight(h -> h
                                    .fields("sku_name", l -> l
                                            .preTags("<em>")
                                            .postTags("</em>")))
                            .source(r -> r
                                    .filter(f -> f
                                            .includes("create_date", "order_price", "province_name",
                                                    "sku_name", "sku_num", "total_amount", "user_age", "user_gender")))
                    , HashMap.class);
        } catch (IOException | ElasticsearchException e) {
            log.info("查询交易明细异常。。");
        }
        ArrayList<Map<String, Object>> sourceMaps = new ArrayList<>();
        if (search != null) {
            long total = search.hits().total().value();
            List<Hit<HashMap>> hits = search.hits().hits();
            hits.forEach(hit -> {
                HashMap source = hit.source();
                Map<String, List<String>> highlight = hit.highlight();
                String sku_name = highlight.get("sku_name").toString();
                source.put("sku_name", sku_name);
                sourceMaps.add(source);
            });
            results.put("total", total);
            results.put("detail", sourceMaps);
        }
        log.info("交易明细：${}", results);
        return results;
    }

    @Override
    public Map<String, Object> searchDau(String td) {
        HashMap<String, Object> dauResults = new HashMap<>();
        //日活总数
        Long dauTotal = searchDauTotal(td);
        dauResults.put("dauTotal", dauTotal);

        //今日分时明细
        Map<String, Long> dauTd = searchDauHr(td);
        dauResults.put("dauTd", dauTd);

        //昨日分时明细
        //计算昨日
        LocalDate tdLd = LocalDate.parse(td);
        LocalDate ydLd = tdLd.minusDays(1);
        Map<String, Long> dauYd = searchDauHr(ydLd.toString());
        dauResults.put("dauYd", dauYd);

        return dauResults;
    }

    public Map<String, Long> searchDauHr(String td) {
        HashMap<String, Long> dauHr = new HashMap<>();
        SearchResponse<HashMap> search = null;
        try {
            search = esClient.search(s -> s
                            .index(dauIndexNamePrefix + td)
                            .aggregations("groupByHr", Aggregation.of(_0 -> _0
                                    .terms(_1 -> _1
                                            .field("hr")
                                            .size(10))))
                    , HashMap.class);
        } catch (IOException | ElasticsearchException e) {
            log.info("查询日活明细异常。。");
        }
        if (search != null) {
            Aggregate groupByHr = search.aggregations().get("groupByHr");
            if (groupByHr != null) {
                List<StringTermsBucket> array = groupByHr.sterms().buckets().array();
                for (StringTermsBucket bucket : array) {
                    String key = bucket.key();
                    long docCount = bucket.docCount();
                    dauHr.put(key, docCount);
                }
            }
        }
        log.info("日活明细：${}", dauHr);
        return dauHr;
    }

    private Long searchDauTotal(String td) {
        SearchResponse<Long> search = null;
        try {
            search = esClient.search(s -> s
                            .index(dauIndexNamePrefix + td)
                            .size(0)
                    , Long.class);
        } catch (IOException e) {
            log.info("查询日活总数异常。。");
        }
        long value = 0;
        if (search != null) {
            TotalHits totalHits = search.hits().total();
            if (totalHits != null) {
                value = totalHits.value();
            }
        }
        log.info("日活总数：${}", value);
        return value;
    }

}
