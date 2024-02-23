package com.yaya;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yaya.base.BaseApp;
import com.yaya.constant.Constant;
import com.yaya.util.DateFormatUtil;
import com.yaya.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class DwdBaseLogApp extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLogApp().start(100,4,"dwd_base_log_app", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务处理
        //1. 对数据进行清洗
        SingleOutputStreamOperator<JSONObject> fliterJson = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        //清洗数据
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            //提取page
                            JSONObject pageObj = jsonObj.getJSONObject("page");
                            //提取start
                            JSONObject startObj = jsonObj.getJSONObject("start");
                            //mid
                            JSONObject commonObj = jsonObj.getJSONObject("common");
                            String mid = commonObj.getString("mid");
                            //ts
                            Long ts = jsonObj.getLong("ts");

                            if ((pageObj != null || startObj != null) && mid != null && ts != null) {
                                out.collect(jsonObj);
                            }

                        } catch (Exception e) {
                            log.warn("出现不完整json数据");
                        }


                    }
                }
        );

        //2. 新老访客状态标记修复-------------------------------------------------------------------------------------------------------
        fliterJson.print();
        SingleOutputStreamOperator<JSONObject> fixIsNewStream = fliterJson.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("mid");
                    }
                }
        ).process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    //定义状态，维护每个mid的首次访问日期
                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> firstVisitStateDesc = new ValueStateDescriptor<>("firstVisitStateDesc", Types.STRING);
                        firstVisitDateState = getRuntimeContext().getState(firstVisitStateDesc);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取值
                        String firstVisitDate = firstVisitDateState.value();
                        // is_new
                        String isNew = value.getJSONObject("common").getString("is_new");
                        //数据时间
                        Long ts = value.getLong("ts");

                        String today = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            if (firstVisitDate == null) {
                                //真实的新访客 , 将数据中的ts对应的日期更新到状态中， 不需要对isNew进行修复
                                firstVisitDateState.update(today);
                            } else if (!firstVisitDate.equals(today)) {
                                // 老访客伪装新访客， 对isNew进行修复
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                // 新访客在同一天重复访问， 不需要进行修复
                            }
                        } else if ("0".equals(isNew)) {
                            if (firstVisitDate == null) {
                                // 老访客， 但是数仓上线后是第一次访问 , 将昨天的日期存入到状态中
                                firstVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                            } else {
                                // 老访问， 状态有值， 不需要做任何处理
                            }
                        } else {
                            //既不是1 也不是0的情况，  数仓中模拟数据不存在该情况。
                        }


                        //写出数据
                        out.collect(value);

                    }
                }
        );


        OutputTag<String> errTag = new OutputTag<>("errTag", Types.STRING);
        OutputTag<String> startTag = new OutputTag<>("startTag", Types.STRING);
        OutputTag<String> displayTag = new OutputTag<>("displayTag", Types.STRING);
        OutputTag<String> actionTag = new OutputTag<>("actionTag", Types.STRING);

        SingleOutputStreamOperator<String> pageStream = fixIsNewStream.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        //提取错误数据
                        JSONObject errObj = value.getJSONObject("err");
                        if (errObj != null) {
                            //写出到侧流
                            ctx.output(errTag, errObj.toJSONString());
                            //从数据中将err移除
                            value.remove("err");
                        }

                        //启动数据
                        JSONObject startObj = value.getJSONObject("start");
                        if (startObj != null) {
                            //启动数据不需要拆分处理，直接写出
                            ctx.output(startTag, value.toJSONString());
                        }

                        //页面数据
                        JSONObject pageObj = value.getJSONObject("page");
                        JSONObject commonObj = value.getJSONObject("common");
                        Long ts = value.getLong("ts");
                        if (pageObj != null) {
                            //分流actions数据
                            JSONArray actionArr = value.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionObj = actionArr.getJSONObject(i);
                                    //补充common
                                    actionObj.put("common", commonObj);
                                    //补充page
                                    actionObj.put("page", pageObj);
                                    //补充ts
                                    actionObj.put("ts", ts);

                                    //写出
                                    ctx.output(actionTag, actionObj.toJSONString());
                                }
                            }

                            //移除actions
                            value.remove("actions");

                            //分流displays数据
                            JSONArray displayArr = value.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayObj = displayArr.getJSONObject(i);
                                    //补充common
                                    displayObj.put("common", commonObj);
                                    //补充page
                                    displayObj.put("page", pageObj);
                                    //补充ts
                                    displayObj.put("ts", ts);

                                    //写出
                                    ctx.output(displayTag, displayObj.toJSONString());
                                }
                            }
                            //移除displays
                            value.remove("displays");


                            //剩下的就是page数据

                            out.collect(value.toJSONString());
                        }
                    }
                }
        );


        //捕获侧流
        SideOutputDataStream<String> errStream = pageStream.getSideOutput(errTag);
        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);

        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink( Constant.TOPIC_DWD_TRAFFIC_PAGE )) ;
        errStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startStream.sinkTo( FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayStream.sinkTo( FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION)) ;







    }
}
