package com.yaya.util;

import com.yaya.constant.Constant;

public class FlinkSQLUtil {
    public static String getKafkaSourceDDL( String topicName , String groupId  ){
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName+  "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS +  "',\n" +
                "  'properties.group.id' = '" + groupId  + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'json.ignore-parse-errors' = 'true' , \n" +
                "  'format' = 'json'\n" +
                ")" ;
    }

    public static String getKafkaSinkDDL(String topicName ){
        return  " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" +topicName+ "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS+ "',\n" +
                "  'sink.delivery-guarantee' = 'exactly-once',\n" +
                "  'sink.transactional-id-prefix' = '"+ topicName+"-"+System.currentTimeMillis() + "'," +
                "  'properties.transaction.timeout.ms' = '600000', \n" +
                "  'format' = 'json'\n" +
                ")"  ;
    }

    public static String getUpsertKafkaSinkDDL(String topicName ){
        return  " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" +topicName+ "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS+ "',\n" +
                "  'key.format' = 'json' ,\n" +
                "  'value.format' = 'json' " +
                ")"  ;
    }
    public static String getDorisSinkDDL( String tableName ) {
        return " WITH (\n" +
                "       'connector' = 'doris',\n" +
                "       'fenodes' = '" + Constant.DORIS_FENODES+ "',\n" +
                "       'table.identifier' = '" + Constant.DORIS_DATABASE + "." + tableName + "',\n" +
                "       'username' = '" +Constant.DORIS_USERNAME+ "',\n" +
                "       'password' = '" + Constant.DORIS_PASSWORD+ "',\n" +
                "       'sink.enable-2pc' = 'true'\n" +
                ")" ;
    }
}