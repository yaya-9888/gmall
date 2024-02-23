package com.yaya;

import com.yaya.base.BaseSQLApp;
import com.yaya.constant.Constant;
import com.yaya.util.FlinkSQLUtil;
import com.yaya.util.FlinkSinkUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10018,4, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //创建topic_db表
        readOdsTopicDb(tableEnv,groupId);

        //获取订单表数据 order_info_table
        Table order_info = tableEnv.sqlQuery("select " +
                "`data`['id'] id ," +
                "`data`['user_id'] user_id ," +
                "`data`['province_id'] province_id," +
                "`data`['operate_time'] operate_time," +
                "`ts`"+
                "from topic_db  " +
                "where  " +
                "`database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status'] = '1002' " +
                "and `data`['order_status']='1005'" );
        //创建order_info表
        tableEnv.createTemporaryView("order_info_table",order_info);

        // 2. 过滤退单表数据 order_refund_info   insert
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_type'] refund_type," +
                        "data['refund_num'] refund_num," +
                        "data['refund_amount'] refund_amount," +
                        "data['refund_reason_type'] refund_reason_type," +
                        "data['refund_reason_txt'] refund_reason_txt," +
                        "data['create_time'] create_time," +
                        "pt," +
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_refund_info' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        //创建base_dic表
        readBaseDic(tableEnv);

        //将订单数据和退单数据进行join 在look up join 字段表
        // 4. join: 普通的和 lookup join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info_table oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");

        // 5. 写出到 kafka
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_REFUND+"(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts bigint," +
                        "PRIMARY KEY (id) NOT ENFORCED" +
                        ")" + FlinkSQLUtil.getUpsertKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND)
        );
        result.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_REFUND).execute();

    }
}
