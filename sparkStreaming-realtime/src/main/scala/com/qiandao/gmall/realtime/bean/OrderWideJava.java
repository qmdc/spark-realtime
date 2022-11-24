package com.qiandao.gmall.realtime.bean;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class OrderWideJava {
    Long detail_id;
    Long order_id;
    Long sku_id;
    Double order_price;
    Long sku_num;
    String sku_name;
    Double split_total_amount;
    Double split_activity_amount;
    Double split_coupon_amount;
    Long province_id;
    String order_status;
    Long user_id;
    Double total_amount;
    Double activity_reduce_amount;
    Double coupon_reduce_amount;
    Double original_total_amount;
    Double feight_fee;
    Double feight_fee_reduce;
    String expire_time;
    String refundable_time;
    String create_time;
    String operate_time;
    String create_date;
    String create_hour;
    String province_name;
    String province_area_code;
    String province_3166_2_code;
    String province_iso_code;
    Integer user_age;
    String user_gender;
}
