package com.yaya;

import com.yaya.base.BaseSQLApp;
import com.yaya.constant.Constant;
import com.yaya.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderCancelDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(10017,4,"dwd_trade_order_cancel_detail");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        //设置ttl时间  15min+5s
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905));

        //1. 读取ods  topic_db的数据
        readOdsTopicDb(tableEnv,groupId);


        //获取订单明细表数据 order_detail_table
        Table order_detail_table = tableEnv.sqlQuery("select " +
                "`data`['id'] id ,\n" +
                "`data`['order_id'] order_id ,\n" +
                "`data`['sku_id'] sku_id ,\n" +
                "`data`['sku_name'] sku_name ,\n" +
                "`data`['img_url'] img_url ,\n" +
                "`data`['order_price'] order_price ,\n" +
                "`data`['sku_num'] sku_num ,\n" +
                "`data`['create_time'] create_time ,\n" +
                "`data`['source_type'] source_type ,\n" +
                "`data`['source_id'] source_id ,\n" +
                "`data`['split_total_amount'] split_total_amount ,\n" +
                "`data`['split_activity_amount'] split_activity_amount ,\n" +
                "`data`['split_coupon_amount'] split_coupon_amount ,\n" +
                "`data`['operate_time'] operate_time \n" +
                "from topic_db " +
                "where `database`='gmall' and `table`='order_detail' and `type`='insert' ");
        //创建 order_detail_table 表
        tableEnv.createTemporaryView("order_detail_table",order_detail_table);



        //获取订单表数据 order_info_table
        Table order_info = tableEnv.sqlQuery("select " +
                "`data`['id'] id ," +
                "`data`['user_id'] user_id ," +
                "`data`['province_id'] province_id," +
                "`data`['operate_time'] operate_time," +
                "`ts`"+
                "from topic_db " +
                "where " +
                "`database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status'] is not  null " +
                "and `data`['order_status']='1003'");
        //创建order_info表
        tableEnv.createTemporaryView("order_info_table",order_info);

        //创建筛选订单明细活动关联表数据
        Table order_detail_activity = tableEnv.sqlQuery("select " +
                "`data`['order_detail_id'] order_detail_id ,\n" +
                "`data`['activity_id'] activity_id ,\n" +
                "`data`['activity_rule_id'] activity_rule_id ,\n" +
                "`data`['sku_id'] sku_id ,\n" +
                "`data`['create_time'] create_time ,\n" +
                "`data`['operate_time'] operate_time " +
                "from topic_db " +
                "where " +
                "`database`='gmall' " +
                "and `table`='order_detail_activity' " +
                "and `type`='insert' ");
        //order_detail_activity_table
        tableEnv.createTemporaryView("order_detail_activity_table",order_detail_activity);


        //筛选订单明细优惠券关联表数据
        Table order_detail_coupon = tableEnv.sqlQuery("select " +
                "`data`['id']  id,\n" +
                "`data`['order_id']  order_id,\n" +
                "`data`['order_detail_id']  order_detail_id,\n" +
                "`data`['coupon_id']  coupon_id,\n" +
                "`data`['coupon_use_id']  coupon_use_id,\n" +
                "`data`['sku_id']  sku_id,\n" +
                "`data`['create_time']  create_time,\n" +
                "`data`['operate_time'] operate_time "+
                "from topic_db " +
                "where " +
                "`database`='gmall' " +
                "and `table`='order_detail_coupon' " +
                "and `type`='insert' ");
        //order_detail_coupon_table
        tableEnv.createTemporaryView("order_detail_coupon_table",order_detail_coupon);

        //将四张表关联
        Table result  = tableEnv.sqlQuery("select " +
                "ot.id id,\n" +
                "ot.order_id order_id,\n" +
                "ot.sku_id sku_id,\n" +
                "ot.sku_name sku_name,\n" +
                "ot.order_price order_price,\n" +
                "ot.sku_num sku_num,\n" +
                "ot.create_time create_time,\n" +
                "ot.split_total_amount split_total_amount,\n" +
                "ot.split_activity_amount split_activity_amount,\n" +
                "ot.split_coupon_amount split_coupon_amount,\n" +
                "oi.user_id user_id,\n" +
                "oi.province_id province_id,\n" +
                "oi.operate_time operate_time,\n" +
                "date_format(oi.operate_time, 'yyyy-MM-dd') order_cancel_date_id,\n" +
                "oa.activity_id activity_id,\n" +
                "oa.activity_rule_id activity_rule_id,\n" +
                "oc.coupon_id coupon_id,\n" +
                "oc.coupon_use_id coupon_use_id," +
                "oi.ts ts\n" +
                "from " +
                "order_detail_table ot " +
                "join order_info_table oi on ot.order_id=oi.id " +
                "left join order_detail_activity_table oa on ot.id=oa.order_detail_id " +
                "left join order_detail_coupon_table oc on ot.id=oc.order_detail_id "

        );

        tableEnv.executeSql("" +
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
                "id STRING,\n" +
                "order_id STRING,\n" +
                "sku_id STRING,\n" +
                "sku_name STRING,\n" +
                "order_price STRING,\n" +
                "sku_num STRING,\n" +
                "create_time STRING,\n" +
                "split_total_amount STRING,\n" +
                "split_activity_amount STRING,\n" +
                "split_coupon_amount STRING,\n" +
                "user_id STRING,\n" +
                "province_id STRING,\n" +
                "operate_time STRING,\n" +
                "order_cancel_date_id STRING,\n" +
                "activity_id STRING,\n" +
                "activity_rule_id STRING,\n" +
                "coupon_id STRING,\n" +
                "coupon_use_id STRING,\n" +
                "ts bigint," +
                "PRIMARY KEY (id) NOT ENFORCED)" + FlinkSQLUtil.getUpsertKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL)
        );


        result.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL).execute();






    }
}
