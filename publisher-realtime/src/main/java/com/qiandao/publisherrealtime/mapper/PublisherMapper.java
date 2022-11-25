package com.qiandao.publisherrealtime.mapper;

import com.qiandao.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface PublisherMapper {

    Map<String, Object> searchDau(String td);

    Map<String, Object> searchDetailByItem(String date, String itemName, int from, Integer pageSize);

    List<NameValue> searchStatsByItemGender(String itemName, String date, String typeToField);

    List<NameValue> searchStatsByItemAge(String itemName, String date, String typeToField);
}
