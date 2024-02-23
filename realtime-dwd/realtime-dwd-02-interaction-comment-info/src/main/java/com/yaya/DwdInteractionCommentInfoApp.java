package com.yaya;

import com.yaya.base.BaseSQLApp;
import com.yaya.constant.Constant;
import com.yaya.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfoApp extends BaseSQLApp {


    public static void main(String[] args) {
        new DwdInteractionCommentInfoApp().start(10012,4,"dwd_interaction_comment_info_app");

    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        // 1. 读取topic_db的数据
        readOdsTopicDb(tableEnv,groupId);

        //筛选出comment_info数据
        Table commentInfoTable = tableEnv.sqlQuery(
                "select  \n" +
                        "`data`['id'] id,\n" +
                        "`data`['user_id'] user_id,\n" +
                        "`data`['nick_name'] nick_name,\n" +
                        "`data`['sku_id'] sku_id,\n" +
                        "`data`['spu_id'] spu_id,\n" +
                        "`data`['order_id'] order_id,\n" +
                        "`data`['appraise'] appraise,\n" +
                        "`data`['comment_txt'] comment_txt,\n" +
                        "`data`['create_time'] create_time,\n" +
                        "`pt` ,\n" +
                        " ts " +
                        "from topic_db \n" +
                        "where `database` = 'gmall'\n" +
                        "and `table` = 'comment_info'\n" +
                        "and `type` = 'insert'\n"
        );
        tableEnv.createTemporaryView("comment_info" , commentInfoTable );


        // 3. 读取base_dic表
        readBaseDic(tableEnv);



        Table joinTable = tableEnv.sqlQuery(
                "select \n" +
                        "ci.id,\n" +
                        "ci.user_id,\n" +
                        "ci.nick_name,\n" +
                        "ci.sku_id,\n" +
                        "ci.spu_id,\n" +
                        "ci.order_id,\n" +
                        "ci.appraise appraise_id, \n" +
                        "bd.info.dic_name appraise_name,\n" +
                        "ci.comment_txt,\n" +
                        "ci.create_time,\n" +
                        "ci.ts " +
                        "from comment_info ci \n" +
                        "join base_dic  \n" +
                        "FOR SYSTEM_TIME AS OF ci.pt as bd\n" +
                        "on  ci.appraise = bd.dic_code "
        );



        tableEnv.executeSql(
                "CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                        "  id STRING ,\n" +
                        "  user_id STRING ,\n" +
                        "  nick_name STRING ,\n" +
                        "  sku_id STRING ,\n" +
                        "  spu_id STRING ,\n" +
                        "  order_id STRING ,\n" +
                        "  appraise_id STRING,\n" +
                        "  appraise_name STRING ,\n" +
                        "  comment_txt STRING ,\n" +
                        "  create_time STRING , \n" +
                        "  ts BIGINT " +
                        ")" + FlinkSQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
        ) ;

        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO ).execute() ;
    }
}
