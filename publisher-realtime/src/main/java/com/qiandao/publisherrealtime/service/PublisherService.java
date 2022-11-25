package com.qiandao.publisherrealtime.service;

import com.qiandao.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface PublisherService {

    Map<String, Object> doDauRealtime(String td);

    Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize);

    List<NameValue> doStatsByItem(String itemName, String date, String t);
}
