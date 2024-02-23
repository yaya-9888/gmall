package com.yaya;

import com.yaya.base.BaseSQLApp;
import com.yaya.constant.Constant;
import com.yaya.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetailApp extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderDetailApp().start(10014,4,"dwd_trade_order_detail_app");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        //创建 topic_db表
        readOdsTopicDb(tableEnv,groupId);

        //2. 筛选订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery(
                "select \n" +
                        "`data`['id'] id , \n" +
                        "`data`['order_id'] order_id , \n" +
                        "`data`['sku_id'] sku_id , \n" +
                        "`data`['sku_name'] sku_name , \n" +
                        "`data`['order_price'] order_price , \n" +
                        "`data`['sku_num'] sku_num , \n" +
                        "`data`['create_time'] create_time , \n" +
                        "`data`['split_total_amount'] split_total_amount , \n" +
                        "`data`['split_activity_amount'] split_activity_amount , \n" +
                        "`data`['split_coupon_amount'] split_coupon_amount , \n" +
                        "`ts`\n" +
                        "from topic_db\n" +
                        "where  `database` = 'gmall'\n" +
                        "and  `table` = 'order_detail'\n" +
                        "and  `type` = 'insert' "
        );
        tableEnv.createTemporaryView("order_detail" , orderDetailTable);

        //3. 筛选订单表数据
        Table orderInfoTable = tableEnv.sqlQuery(
                "select \n" +
                        "`data`['id'] id ," +
                        "`data`['user_id'] user_id , \n" +
                        "`data`['province_id'] province_id \n" +
                        "from topic_db\n" +
                        "where  `database` = 'gmall'\n" +
                        "and  `table` = 'order_info'\n" +
                        "and  `type` = 'insert' "
        );

        tableEnv.createTemporaryView("order_info" , orderInfoTable);


        //4. 筛选订单明细活动关联表
        Table orderDetailActivityTable = tableEnv.sqlQuery(
                "select \n" +
                        "`data`['order_detail_id'] order_detail_id ,\n" +
                        "`data`['activity_id'] activity_id , \n" +
                        "`data`['activity_rule_id'] activity_rule_id  \n" +
                        "from topic_db\n" +
                        "where  `database` = 'gmall'\n" +
                        "and  `table` = 'order_detail_activity'\n" +
                        "and  `type` = 'insert'"
        );

        tableEnv.createTemporaryView("order_detail_activity" , orderDetailActivityTable) ;

        //5. 筛选订单明细优惠券关联表
        Table orderDetailCouponTable = tableEnv.sqlQuery(
                "select \n" +
                        "`data`['order_detail_id'] order_detail_id ,\n" +
                        "`data`['coupon_id'] coupon_id \n" +
                        "from topic_db\n" +
                        "where  `database` = 'gmall'\n" +
                        "and  `table` = 'order_detail_coupon'\n" +
                        "and  `type` = 'insert' "
        );

        tableEnv.createTemporaryView("order_detail_coupon" ,orderDetailCouponTable );

        Table joinTable = tableEnv.sqlQuery(
                "select \n" +
                        "od.id , \n" +
                        "od.order_id , \n" +
                        "od.sku_id , \n" +
                        "od.sku_name , \n" +
                        "od.order_price , \n" +
                        "od.sku_num , \n" +
                        "od.create_time , \n" +
                        "od.split_total_amount , \n" +
                        "od.split_activity_amount , \n" +
                        "od.split_coupon_amount , \n" +
                        "od.ts,\n" +
                        "oi.user_id user_id ,\n" +
                        "oi.province_id province_id , \n" +
                        "oda.activity_id activity_id ,\n" +
                        "oda.activity_rule_id activity_rule_id ,\n" +
                        "odc.coupon_id coupon_id \n" +
                        "from \n" +
                        "order_detail od \n" +
                        "join  order_info  oi  on od.order_id = oi.id \n" +
                        "left join order_detail_activity oda  on od.id = oda.order_detail_id\n" +
                        "left join order_detail_coupon odc on od.id = odc.order_detail_id"
        );
        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+ "(\n" +
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
                        "ts BIGINT,\n" +
                        "user_id STRING,\n" +
                        "province_id STRING,\n" +
                        "activity_id STRING,\n" +
                        "activity_rule_id STRING,\n" +
                        "coupon_id STRING,\n" +
                        "PRIMARY KEY (id) NOT ENFORCED\n" +
                        ")" + FlinkSQLUtil.getUpsertKafkaSinkDDL( Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        ) ;

        joinTable.insertInto( Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute() ;




    }
}
