package com.yaya;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yaya.base.BaseApp;
import com.yaya.bean.TableProcessDim;
import com.yaya.constant.Constant;
import com.yaya.util.FlinkSourceUtil;
import com.yaya.util.HBaseUtil;
import com.yaya.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.*;

@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //{"database":"gmall","table":"activity_info","type":"bootstrap-insert","ts":1704106296,
        // "data":{"id":2,"activity_name":"CAREMiLLE口红满两个8折","activity_type":"3102","activity_desc":"CAREMiLLE口红满两个8折","start_time":"2022-01-13 01:01:54","end_time":"2023-06-19 00:00:00","create_time":"2022-05-27 00:00:00","operate_time":null}}
        //对读取的数据进行清洗过滤
        // Topic_db的数据maxwall同步的    maxwall全量同步有类型有无用数据
        SingleOutputStreamOperator<JSONObject> etlStream = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                        //防止解析json不全报错 将报错打印到日志中
                        try {
                            //将topic_db读取的数据转换成json
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            //取出数据来自的数据库
                            String dataname = jsonObject.getString("database");
                            //数据变更类型
                            String type = jsonObject.getString("type");
                            //数据
                            JSONObject data = jsonObject.getJSONObject("data");

                            if ("gmall".equals(dataname)
                                    && !"bootstrap-start".equals(type)
                                    && !"bootstrap-complete".equals(type)
                                    && data.size() > 0
                                    && data != null) {
                                collector.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            log.warn("过滤掉脏数据: " + value);
                        }

                    }
                }
        );

        etlStream.print("etl");

        //3.  读取配置表的数据， 动态处理 Hbase中的维度表
        //使用flinkCDC读取配置表数据
        DataStreamSource<String> mysqlcdc = env.
                fromSource(FlinkSourceUtil.getMysqlSource(Constant.TABLE_PROCESS_DATABASE, Constant.TABLE_PROCESS_DIM),
                        WatermarkStrategy.noWatermarks(),
                        "mysqlcdc");

        //根据json数据封装对象  类型op: c（增）  r（读）u（改）  d（删）
        // c r u 新的数据都在 after
        // d 删除的数据在 before   按照规则封装数据对象
        SingleOutputStreamOperator<TableProcessDim> tableProcessDimStream = mysqlcdc.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String s) throws Exception {
                        //string 转换成json
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        //获取类型
                        String op = jsonObject.getString("op");
                        //提出返回对象
                        TableProcessDim tableProcessDim;
                        //判断删还是增读改
                        if ("d".equals(op)) {
                            //可以将json里的下划线的字段名 对应上驼峰命名
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            // c r u 获取after里面数据
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);

                        return tableProcessDim;

                    }
                }
        );
        //根据配置表转换成的TableProcessDim对象 来创建和删除Hbase的表
        // 使用复函数 生命周期来创建 Hbase连接  使用map数据输入输出无操作
        SingleOutputStreamOperator<TableProcessDim> createOrDropTableStream = tableProcessDimStream.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        connection = HBaseUtil.getConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeConnection(connection);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        //判断类型
                        String op = tableProcessDim.getOp();
                        //根据op的值，决定建表还是删表
                        if ("d".equals(op)) {
                            //删除表
                            dropHBaseTable(tableProcessDim);
                        } else if ("u".equals(op)) {
                            //删除表
                            dropHBaseTable(tableProcessDim);
                            //建表
                            createHBaseTable(tableProcessDim);
                        } else {
                            // c r
                            //建表
                            createHBaseTable(tableProcessDim);
                        }
                        return tableProcessDim;
                    }

                    //获取对象中的列组和静态常量的表 进行删除穿件操作
                    private void createHBaseTable(TableProcessDim tableProcessDim) throws IOException {
                        String[] cfs = tableProcessDim.getSinkFamily().split(",");
                        HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), cfs);
                    }

                    private void dropHBaseTable(TableProcessDim tableProcessDim) throws IOException {
                        HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                    }
                }
        );

        //创建配置流处理成广播流
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDesc", Types.STRING, Types.POJO(TableProcessDim.class));
        BroadcastStream<TableProcessDim> broadcastStream = createOrDropTableStream.broadcast(mapStateDescriptor);

        //将topic_db和广播流进行connect连接
        //过滤出需要的表  因为cdc慢 先用初始化连接mysql读取数据   放到map中 先读map在用状态
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = etlStream.connect(broadcastStream).process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
            //预加载的配置表数据
            private Map<String, TableProcessDim> preConfigMap;

            @Override
            public void open(Configuration parameters) throws Exception {
                //基于生命周期open方法会早于processElement方法执行，
                //可以在该方法中预加载配置表数据，存入到一个集合中 , 配合状态来使用
                //Flink CDC 初始启动会比较慢， 主流数据早于配置流， 主流中本应该定性为维度表的数据 ， 因为状态中还没有存入维度表信息，
                //而最终定性为非维度表数据

                //通过jdbc编码完成预加载
                java.sql.Connection connection = JdbcUtil.getConnection();
                List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(
                        connection,
                        "select `source_table` , `sink_table` , `sink_family` , `sink_columns`, `sink_row_key` from gmall_config.table_process_dim "
                        ,
                        TableProcessDim.class
                        ,
                        true
                );
                //为了方便查询，转换成map结构
                preConfigMap = new HashMap<>();

                for (TableProcessDim tableProcessDim : tableProcessDimList) {
                    preConfigMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                }

                JdbcUtil.closeConnection(connection);

            }

            @Override
            public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                //获取广播流状态
                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //从数据中获取表名
                String table = value.getString("table");
                //根据表名尝试获取状态
                TableProcessDim tableProcessDim = broadcastState.get(table);
                //根据tableProcessDim是否为空， 决定是否要从预加载的map中再次读取

                if (tableProcessDim == null) {
                    tableProcessDim = preConfigMap.get(table);
                    log.info("从预加载的Map中读取维度信息");
                }
                // 不为空将数据输出
                if (tableProcessDim != null) {
                    // topic_db  和 取到的配置数据
                    out.collect(Tuple2.of(value, tableProcessDim));
                    System.out.println(tableProcessDim.toString());
                }

            }

            @Override
            public void processBroadcastElement(TableProcessDim value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                //获取广播状态
                BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //判断d就将状态清楚调  cdc字段有主见会先删在增 不是改
                if ("d".equals(value.getOp())) {
                    broadcastState.remove(value.getSourceTable());
                    //同步删除预加载Map中的数据
//                    preConfigMap.remove(value.getSourceTable());
                    //c r u 添加状态
                } else {
                    broadcastState.put(value.getSourceTable(), value);
                }

            }
        }).setParallelism(4);
        dimStream.print("-------------");

        //dimStream  过滤完表  开始过滤表中的字段  k是数据 v是配置  根据v的配置过滤k中的数据

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterdatasink = dimStream.map(
                new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                        JSONObject json = value.f0;
                        TableProcessDim tableProcessDim = value.f1;

                        //获取数据json中data中的字段 跟TableProcessDim需要怎么字段进行移除
                        JSONObject data = json.getJSONObject("data");
                        //获取data字段
                        Set<String> dataAllKeys = data.keySet();

                        //获取配置表需要的字段
                        String sinkColumns = tableProcessDim.getSinkColumns();
                        //获取每个字段
                        List<String> sinkColumnList = Arrays.asList(sinkColumns.split(","));

                        //将配置中没有的字段进行移除
                        dataAllKeys.removeIf(key -> !sinkColumnList.contains(key));


                        return Tuple2.of(json, tableProcessDim);
                    }
                }
        );
        filterdatasink.print("过滤完的数据");

        //将数据插入或删除Hbase中数据
        filterdatasink.addSink(
                new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
                    Connection connection;

                    //生命周期初始化
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //创建Hbase连接
                        connection = HBaseUtil.getConnection();
                    }

                    //关闭连接
                    @Override
                    public void close() throws Exception {
                        //关闭Hbase连接
                        HBaseUtil.closeConnection(connection);
                    }

                    @Override
                    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
                        //topic_db 过滤后的json数据
                        JSONObject jsonObject = value.f0;
                        //配置表里面封装的对象
                        TableProcessDim tableProcessDim = value.f1;

                        //数据的操作类型
                        String type = jsonObject.getString("type");
                        //获取json里面的type 判读是否删除
                        JSONObject data = jsonObject.getJSONObject("data");

                        //根据类型，决定是 put 还是 delete
                        if ("delete".equals(type)) {
                            //从hbase的维度表中删除维度数据
                            deleteDimData(data, tableProcessDim);
                        } else {
                            //insert  update  bootstrap-insert
                            putDimData(data, tableProcessDim);
                            System.out.println("添加数据----------------------添加数据");
                        }

                    }

                    private void putDimData(JSONObject dataObj, TableProcessDim tableProcessDim) throws IOException {
                        //获取rowkey
                        //获取配置表里的rowkey字段名
                        String sinkRowKeyName = tableProcessDim.getSinkRowKey();
                        //使用获取的字段名去数据里获取 数据的rowkey值
                        String sinkRowKeyValue = dataObj.getString(sinkRowKeyName);

                        //cf 获取列组
                        String cf = tableProcessDim.getSinkFamily();
                        HBaseUtil.putCells(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), sinkRowKeyValue, cf, dataObj);
                    }

                    private void deleteDimData(JSONObject dataObj, TableProcessDim tableProcessDim) throws IOException {
                        //rowkey
                        String sinkRowKeyName = tableProcessDim.getSinkRowKey();
                        String sinkRowKeyValue = dataObj.getString(sinkRowKeyName);
                        HBaseUtil.deleteCells(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), sinkRowKeyValue);
                    }
                }
        );


    }
}
