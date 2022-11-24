package com.qiandao.gmall.realtime.bean;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class DauInfoJava {
    //基本的页面访问日志的数据
    String mid;
    String user_id;
    String province_id;
    String channel;
    String is_new;
    String model;
    String operate_system;
    String version_code;
    String brand;
    String page_id;
    String page_item;
    String page_item_type;
    String sourceType;
    Long during_time;

    //用户性别 年龄
    String user_gender;
    String user_age;

    //地区信息
    String province_name;
    String province_iso_code;
    String province_3166_2;
    String province_area_code;

    //日期
    String dt;
    String hr;
    Long ts;
}
