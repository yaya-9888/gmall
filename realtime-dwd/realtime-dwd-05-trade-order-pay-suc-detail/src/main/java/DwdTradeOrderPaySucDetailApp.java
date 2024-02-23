import com.yaya.base.BaseSQLApp;
import com.yaya.constant.Constant;
import com.yaya.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetailApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetailApp().start(10015,4,"dwd_trade_order_pay_suc_detail_app");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //创建topic_db表
        readOdsTopicDb(tableEnv,groupId);


        //2. 筛选支付成功数据
        Table paymentInfoTable = tableEnv.sqlQuery(
                "select\n" +
                        "`data`['out_trade_no'] out_trade_no ,\n" +
                        "`data`['order_id'] order_id ,\n" +
                        "`data`['payment_type'] payment_type ,\n" +
                        "`data`['trade_no'] trade_no ,\n" +
                        "`data`['total_amount'] total_amount ,\n" +
                        "`data`['subject'] subject ,\n" +
                        "`data`['payment_status'] payment_status ,\n" +
                        "`data`['create_time'] create_time ,\n" +
                        "`data`['callback_time'] callback_time ,\n" +
                        "`data`['callback_content'] callback_content ,\n" +
                        "`data`['operate_time'] operate_time ,\n" +
                        "`ts` ,\n" +
                        "`et` , " +
                        "`pt` " +
                        "from topic_db\n" +
                        "where `database` = 'gmall'\n" +
                        "and `table` = 'payment_info'\n" +
                        "and `type` = 'update'\n" +
                        "and `data`['payment_status'] is not null \n" +
                        "and `data`['payment_status'] ='1602' "
        );

        tableEnv.createTemporaryView("payment_info" ,paymentInfoTable);

        //3. 读取DWD 下单事务事实表数据
        tableEnv.executeSql(
                "create table order_detail(\n" +
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
                        "coupon_id STRING , \n" +
                        "et AS TO_TIMESTAMP_LTZ( ts * 1000 , 3 ) , " +
                        "watermark for et AS et - INTERVAL '5' SECOND " +
                        ")" + FlinkSQLUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL , groupId )
        ) ;

        //4. 读取字典表
        readBaseDic(tableEnv);

        Table joinTable = tableEnv.sqlQuery(
                "select\n" +
                        "od.id ,\n" +
                        "od.order_id ,\n" +
                        "od.sku_id ,\n" +
                        "od.sku_name ,\n" +
                        "od.order_price ,\n" +
                        "od.sku_num ,\n" +
                        "od.split_total_amount ,\n" +
                        "od.split_activity_amount ,\n" +
                        "od.split_coupon_amount ,\n" +
                        "od.user_id ,\n" +
                        "od.province_id ,\n" +
                        "od.activity_id ,\n" +
                        "od.activity_rule_id ,\n" +
                        "od.coupon_id ,\n" +
                        "pi.out_trade_no,\n" +
                        "pi.payment_type payment_type_code ,\n" +
                        "bd.info.dic_name payment_type_name ,\n" +
                        "pi.trade_no,\n" +
                        "pi.total_amount,\n" +
                        "pi.subject,\n" +
                        "pi.payment_status,\n" +
                        "pi.create_time,\n" +
                        "pi.callback_time,\n" +
                        "pi.callback_content,\n" +
                        "pi.operate_time,\n" +
                        "pi.ts\n" +
                        "from order_detail od  join  payment_info pi \n" +
                        "on od.order_id = pi.order_id \n" +
                        "and  pi.et between od.et - INTERVAL '5' SECOND AND  od.et + INTERVAL '15' MINUTE \n" +
                        "join base_dic FOR SYSTEM_TIME AS OF pi.pt AS bd \n" +
                        "on pi.payment_type =bd.dic_code"

        );



        //写出到kafka
        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+ "(\n" +
                        "id STRING ,\n" +
                        "order_id STRING ,\n" +
                        "sku_id STRING ,\n" +
                        "sku_name STRING ,\n" +
                        "order_price STRING ,\n" +
                        "sku_num STRING ,\n" +
                        "split_total_amount STRING ,\n" +
                        "split_activity_amount STRING ,\n" +
                        "split_coupon_amount STRING ,\n" +
                        "user_id STRING ,\n" +
                        "province_id STRING ,\n" +
                        "activity_id STRING ,\n" +
                        "activity_rule_id STRING ,\n" +
                        "coupon_id STRING ,\n" +
                        "out_trade_no STRING ,\n" +
                        "payment_type_code STRING ,\n" +
                        "payment_type_name STRING ,\n" +
                        "trade_no STRING ,\n" +
                        "total_amount STRING ,\n" +
                        "subject STRING ,\n" +
                        "payment_status STRING ,\n" +
                        "create_time STRING ,\n" +
                        "callback_time STRING ,\n" +
                        "callback_content STRING ,\n" +
                        "operate_time STRING ,\n" +
                        "ts BIGINT \n" +
                        ")"  + FlinkSQLUtil.getKafkaSinkDDL( Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS)
        ) ;
        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS ).execute();


    }
}
