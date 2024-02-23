package com.yaya.base;


import com.yaya.constant.Constant;
import com.yaya.util.FlinkSQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;


public abstract  class BaseSQLApp {
    /**
     * 子类实现，用于核心逻辑的处理
     */
    public abstract void handle( StreamTableEnvironment tableEnv , StreamExecutionEnvironment env  , String groupId  );

    /**
     * 控制整个代码的流程
     */
    public void start (int port , int parallelism , String ckAndGroupId )   {
        //操作HDFS用户
        System.setProperty("HADOOP_USER_NAME" , Constant.HDFS_USER_NAME) ;

        Configuration conf = new Configuration();
        conf.setInteger("rest.port" , port);

        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度
        env.setParallelism(parallelism) ;

        // 状态后端的设置
        env.setStateBackend( new HashMapStateBackend()) ;

        // 开启检查点以及检查点配置
        env.enableCheckpointing(2000) ;
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //  checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2024/stream/" + ckAndGroupId);
        //  checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //  checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        // 其他检查点配置

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //数据转换处理
        handle(  tableEnv  ,  env , ckAndGroupId  );

//        //启动执行
//        try {
//            env.execute() ;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }

    }
    public void readOdsTopicDb( StreamTableEnvironment tableEnv  , String groupId ){
        tableEnv.executeSql(
                "CREATE TABLE topic_db (\n" +
                        "  `database` STRING,\n" +
                        "  `table` STRING,\n" +
                        "  `type` STRING,\n" +
                        "  `ts` BIGINT,\n" +
                        "  `data` MAP<STRING,STRING>,\n" +
                        "  `old` MAP<STRING,STRING>,\n" +
                        "  `pt` as PROCTIME(), \n" +
                        "  `et` AS TO_TIMESTAMP_LTZ( ts * 1000 , 3 ) , "+
                        "   watermark for et AS et - INTERVAL '5' SECOND " +
                        ") " + FlinkSQLUtil.getKafkaSourceDDL(Constant.TOPIC_DB , groupId  )
        ) ;

    }

    public void readBaseDic( StreamTableEnvironment tableEnv ){
        tableEnv.executeSql(
                "CREATE TABLE base_dic (\n" +
                        " dic_code STRING,\n" +
                        " info ROW<dic_name STRING>,\n" +
                        " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'hbase-2.2',\n" +
                        " 'table-name' = 'gmall_realtime:dim_base_dic',\n" +
                        " 'zookeeper.quorum' = '" + Constant.HBASE_ZOOKEEPER_QUORUM+ "'\n" +
                        ");\n"
        ) ;
    }
}
