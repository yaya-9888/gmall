import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.yaya.base.BaseApp;
import com.yaya.bean.TableProcessDwd;
import com.yaya.constant.Constant;
import com.yaya.util.FlinkSinkUtil;
import com.yaya.util.FlinkSourceUtil;
import com.yaya.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DwdBaseDbApp extends BaseApp {

    public static void main(String[] args) {
        new DwdBaseDbApp().start(10016,4,"dwd_base_db_app", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1.对数据进行清洗
        SingleOutputStreamOperator<JSONObject> etlStream = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            String database = jsonObj.getString("database");
                            String type = jsonObj.getString("type");
                            JSONObject dataObj = jsonObj.getJSONObject("data");

                            if ("gmall".equals(database)
                                    && ("insert".equals(type) || "update".equals(type))
                                    && dataObj != null && dataObj.size() > 0) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            log.warn("过滤掉脏数据: " + value);
                        }
                    }
                }
        );

        //2. 读取配置表的数据
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource(Constant.TABLE_PROCESS_DATABASE, Constant.TABLE_PROCESS_DWD);
        DataStreamSource<String> configStream = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");

        //将配置表数据封装带 TableProcessDwd对象中
        SingleOutputStreamOperator<TableProcessDwd> tableProcessDwdStream = configStream.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String value) throws Exception {
                        JSONObject jsonObj = JSONObject.parseObject(value);
                        String op = jsonObj.getString("op");
                        TableProcessDwd tableProcessDwd;
                        if ("d".equals(op)) {
                            tableProcessDwd = jsonObj.getObject("before", TableProcessDwd.class);
                        } else {
                            // c r u
                            tableProcessDwd = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tableProcessDwd.setOp(op);

                        return tableProcessDwd;
                    }
                }
        );
        //3.将配置流处理成广播流，
        MapStateDescriptor<String, TableProcessDwd> mapStateDesc =
                new MapStateDescriptor<>("mapStateDesc", Types.STRING, Types.POJO(TableProcessDwd.class));
        BroadcastStream<TableProcessDwd> broadcastStream = tableProcessDwdStream.broadcast(mapStateDesc);

        //4.主流 connect 广播流  ，动态处理事实表数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dwdStream = etlStream.connect(broadcastStream)
                .process(
                        new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                            Map<String, TableProcessDwd> preConfigMap;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                Connection connection = JdbcUtil.getConnection();
                                List<TableProcessDwd> tableProcessDwdList =
                                        JdbcUtil.queryList(connection, "select * from gmall_config.table_process_dwd", TableProcessDwd.class, true);

                                //将查询到的配置数据存入到map中
                                preConfigMap = new HashMap<>();
                                for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
                                    preConfigMap.put(getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType()), tableProcessDwd);
                                }
                                JdbcUtil.closeConnection(connection);
                            }

                            @Override
                            public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                                //从数据中提取表名 和 类型
                                String tableName = value.getString("table");
                                String type = value.getString("type");
                                String key = getKey(tableName, type);
                                //获取状态
                                ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDesc);

                                //从状态中读取对应的数据
                                TableProcessDwd tableProcessDwd = broadcastState.get(key);

                                if (tableProcessDwd == null) {
                                    // 从预加载的Map中读取数据
                                    tableProcessDwd = preConfigMap.get(key);
                                }

                                if (tableProcessDwd != null) {
                                    //写出数据
                                    out.collect(Tuple2.of(value.getJSONObject("data"), tableProcessDwd));
                                }
                            }

                            /**
                             * 处理广播流的数据
                             *
                             * 就是按照配置流中的数据的类型 c r u d ，决定将数据存入状态还是从状态中删除数据
                             */
                            @Override
                            public void processBroadcastElement(TableProcessDwd value, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                                BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDesc);
                                String op = value.getOp();
                                if ("d".equals(op)) {
                                    //从状态中删除对应的数据
                                    broadcastState.remove(getKey(value.getSourceTable(), value.getSourceType()));
                                    //从map中同步删除数据
                                    preConfigMap.remove( getKey(value.getSourceTable(), value.getSourceType()) );
                                } else {
                                    // c r u
                                    //将数据存入到状态中
                                    broadcastState.put(getKey(value.getSourceTable(), value.getSourceType()), value);
                                }
                            }

                            private String getKey(String sourceTable, String sourceType) {
                                return sourceTable + ":" + sourceType ;
                            }
                        }
                );

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> filterSinkColumnStream = dwdStream.map(
                new MapFunction<Tuple2<JSONObject, TableProcessDwd>, Tuple2<JSONObject, TableProcessDwd>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcessDwd> map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                        JSONObject dataObj = value.f0;
                        TableProcessDwd tableProcessDwd = value.f1;
                        List<String> sinkColumnList = Arrays.asList(tableProcessDwd.getSinkColumns().split(","));
                        dataObj.keySet().removeIf(key -> !sinkColumnList.contains(key));

                        return value;
                    }
                }
        );

        filterSinkColumnStream.sinkTo(FlinkSinkUtil.getKafkaSink()) ;




    }
}
