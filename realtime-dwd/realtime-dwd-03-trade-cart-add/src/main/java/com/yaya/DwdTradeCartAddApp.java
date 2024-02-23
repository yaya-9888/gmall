package com.yaya;

import com.yaya.base.BaseSQLApp;
import com.yaya.constant.Constant;
import com.yaya.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAddApp extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAddApp().start(10013,4,"dwd_trade_cart_add_app");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //1. 读取ods  topic_db的数据
        readOdsTopicDb( tableEnv , groupId);

        Table cartAddTable = tableEnv.sqlQuery("select \n" +
                "`data`['id'] id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['sku_id'] sku_id,\n" +
                "`data`['cart_price'] cart_price,\n" +
                " if( `type`='insert' , cast( `data`['sku_num'] as int) , cast(`data`['sku_num'] as int ) - cast( `old`['sku_num'] as int ) ) sku_num,\n" +
                "`data`['sku_name'] sku_name,\n" +
                "`data`['is_checked'] is_checked,\n" +
                "`data`['create_time'] create_time,\n" +
                "`data`['operate_time'] operate_time,\n" +
                "`data`['is_ordered'] is_ordered,\n" +
                "`data`['order_time'] order_time,\n" +
                "`data`['source_type'] source_type,\n" +
                "`data`['source_id'] source_id , \n" +
                " ts " +
                "from topic_db \n" +
                "where `database` = 'gmall'\n" +
                "and  `table` = 'cart_info'\n" +
                "and  \n" +
                "( `type` = 'insert' \n" +
                "   or \n" +
                "   ( `type` = 'update' and `old`['sku_num'] is not  null and  cast( `data`['sku_num']  as int ) > cast(`old`['sku_num'] as int ) )\n" +
                ")"
        );

        tableEnv.executeSql(
                "create table  " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                        "  id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  sku_id STRING,\n" +
                        "  cart_price STRING,\n" +
                        "  sku_num INT,\n" +
                        "  sku_name STRING,\n" +
                        "  is_checked STRING,\n" +
                        "  create_time STRING,\n" +
                        "  operate_time STRING,\n" +
                        "  is_ordered STRING,\n" +
                        "  order_time STRING,\n" +
                        "  source_type STRING,\n" +
                        "  source_id STRING,\n" +
                        "  ts BIGINT\n" +
                        ")" + FlinkSQLUtil.getKafkaSinkDDL( Constant.TOPIC_DWD_TRADE_CART_ADD)
        ) ;

        cartAddTable.insertInto( Constant.TOPIC_DWD_TRADE_CART_ADD).execute() ;


    }
}
