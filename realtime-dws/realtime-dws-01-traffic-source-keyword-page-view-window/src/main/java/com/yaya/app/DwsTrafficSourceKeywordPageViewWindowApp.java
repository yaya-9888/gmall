package com.yaya.app;

import com.yaya.base.BaseSQLApp;
import com.yaya.constant.Constant;
import com.yaya.function.SplitWordFunction;
import com.yaya.util.FlinkSQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindowApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindowApp().start( 10021 , 4 ,  "dws_traffic_source_keyword_page_view_window_app");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //1.从Kafka topic_dwd_traffic_page主题读取页面数据
        tableEnv.executeSql(
                "CREATE TABLE page_info (\n" +
                        "  `page` MAP<STRING,STRING>,\n" +
                        "  `ts` BIGINT,\n" +
                        "  `et` as TO_TIMESTAMP_LTZ(ts , 3) ,\n" +
                        "   WATERMARK FOR et AS et - INTERVAL '5' SECOND \n" +
                        ") " + FlinkSQLUtil.getKafkaSourceDDL( Constant.TOPIC_DWD_TRAFFIC_PAGE , groupId)
        );

        // 2. 筛选出搜索的数据
        Table searchTable = tableEnv.sqlQuery(
                "select \n" +
                        "  `page`['item']  keyword, \n" +
                        "   et \n" +
                        "from page_info \n" +
                        "where `page`['last_page_id'] in ('search' , 'home')\n" +
                        " and  `page`['item_type']  = 'keyword'\n" +
                        " and  `page`['item'] is not null "
        );


        tableEnv.createTemporaryView("search_info" , searchTable) ;

        //3. 注册函数
        tableEnv.createTemporaryFunction( "SPLITWORD" , SplitWordFunction.class);

        //4. 调用函数，进行分词处理，
        Table keywordTable = tableEnv.sqlQuery(
                " SELECT  word  , et  " +
                        " FROM search_info " +
                        " LEFT JOIN LATERAL TABLE(SPLITWORD( keyword )) ON TRUE"
        );

        tableEnv.createTemporaryView("keyword_info" , keywordTable) ;




        //5.开窗，求每个word的次数
        Table windowTable = tableEnv.sqlQuery(
                " SELECT " +
                        "  DATE_FORMAT( window_start, 'yyyy-MM-dd HH:mm:ss') stt ," +
                        "  DATE_FORMAT( window_end, 'yyyy-MM-dd HH:mm:ss' ) edt  ," +
                        "  DATE_FORMAT( NOW() , 'yyyyMMdd') cur_date , " +
                        "  word keyword," +
                        "  count(word) keyword_count " +
                        "  FROM TABLE( " +
                        "     TUMBLE(TABLE keyword_info, DESCRIPTOR(et), INTERVAL '10' SECOND)) " +
                        "  GROUP BY window_start, window_end , word "
        );

        windowTable.execute().print();


        //6. 写出到Doris中
        tableEnv.executeSql(
                " create table " + Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW  + " (\n" +
                        "   `stt` STRING ,\n" +
                        "   `edt` STRING , \n" +
                        "   `cur_date` STRING ,\n" +
                        "   `keyword` STRING , \n" +
                        "   `keyword_count` BIGINT \n" +
                        "  ) " + FlinkSQLUtil.getDorisSinkDDL( Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW )
        ) ;

        windowTable.insertInto( Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW ).execute() ;
    }
}
