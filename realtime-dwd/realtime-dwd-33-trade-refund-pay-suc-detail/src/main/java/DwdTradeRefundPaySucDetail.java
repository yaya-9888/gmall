import com.yaya.base.BaseSQLApp;
import com.yaya.constant.Constant;
import com.yaya.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeRefundPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(10019,4, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //获取topic_db
        readOdsTopicDb(tableEnv,groupId);

        //过滤退单表数据 order_refund_info   insert
        Table order_refund_info = tableEnv.sqlQuery(
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
                        "and `table`='order_refund_info'" +
                        "and data['refund_status']='0705' " +
                        "and `old`['refund_status'] is not null " +
                        "and `type`='update' ");
        tableEnv.createTemporaryView("order_refund_info", order_refund_info);

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
                "and `old`['order_status']  is not null " +
                "and `data`['order_status']='1006'" );
        //创建order_info表
        tableEnv.createTemporaryView("order_info_table",order_info);

        //退款表
        Table refund_payment = tableEnv.sqlQuery(
                "select " +
                        "`data`['id'] id ,\n" +
                        "`data`['out_trade_no'] out_trade_no ,\n" +
                        "`data`['order_id'] order_id ,\n" +
                        "`data`['sku_id'] sku_id ,\n" +
                        "`data`['payment_type'] payment_type ,\n" +
                        "`data`['trade_no'] trade_no ,\n" +
                        "`data`['total_amount'] total_amount ,\n" +
                        "`data`['subject'] subject ,\n" +
                        "`data`['refund_status'] refund_status ,\n" +
                        "`data`['create_time'] create_time ,\n" +
                        "`data`['callback_time'] callback_time ,\n" +
                        "`data`['callback_content'] callback_content ,\n" +
                        "`data`['operate_time'] operate_time ,\n" +
                        "pt," +
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='refund_payment'" +
                        "and data['refund_status']='1602' " +
                        "and `old`['refund_status'] is not null " +
                        "and `type`='update' ");

        tableEnv.createTemporaryView("refund_payment", refund_payment);

        //创建base_dic表
        readBaseDic(tableEnv);

        Table result = tableEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info_table oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.pt as dic " +
                        "on rp.payment_type=dic.dic_code ");

        // 7.写出到 kafka
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS+"(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint " +
                ")" + FlinkSQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));

        result.insertInto(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS).execute();


    }
}
