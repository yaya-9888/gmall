package com.yaya;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yaya.base.BaseApp;
import com.yaya.bean.UserLoginBean;
import com.yaya.constant.Constant;
import com.yaya.function.DorisMapFunction;
import com.yaya.util.DateFormatUtil;
import com.yaya.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

@Slf4j
public class DwsUserUserLoginWindowApp  extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindowApp().start(
                10024,
                4,
                "dws_user_user_login_window_app" ,
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //过滤数据
        SingleOutputStreamOperator<JSONObject> streamOperator = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            String uid = jsonObj.getJSONObject("common").getString("uid");
                            String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");

                            if (uid != null && (lastPageId == null || "login".equals(lastPageId))) {
                                out.collect(jsonObj);
                            }

                        } catch (Exception e) {
                            log.warn("过滤掉脏数据: " + value);
                        }
                    }
                }
        );

        //2. 按照uid分组， 判断回流和独立用户 封装对象中
        SingleOutputStreamOperator<UserLoginBean> uuBackStream = streamOperator.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("uid");
                    }
                }
        ).process(
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                    private ValueState<String> lastLoginState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastLoginDesc", Types.STRING);
                        lastLoginState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                        //从状态中取值
                        String lastLoginDt = lastLoginState.value();
                        //今天
                        Long ts = value.getLong("ts");
                        String todayDt = DateFormatUtil.tsToDate(ts);

                        Long uuCt = 0L;
                        Long backCt = 0L;

                        if (!todayDt.equals(lastLoginDt)) {
                            //独立用户
                            uuCt = 1L;
                            //进一步判断是否是回流用户
                            if (lastLoginDt != null && (ts - DateFormatUtil.dateToTs(lastLoginDt)) > 7 * 24 * 60 * 60 * 1000) {
                                backCt = 1L;
                            }
                            //更新状态
                            lastLoginState.update(todayDt);
                        }

                        if (uuCt == 1L) {
                            UserLoginBean userLoginBean =
                                    UserLoginBean.builder()
                                            .uuCt(uuCt)
                                            .backCt(backCt)
                                            .ts(ts)
                                            .build();

                            out.collect(userLoginBean);
                        }
                    }
                }
        );

        //获取水位线
        SingleOutputStreamOperator<UserLoginBean> withWatermarkStream = uuBackStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<UserLoginBean>() {
                                    @Override
                                    public long extractTimestamp(UserLoginBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )

        );

        //开窗汇总
        SingleOutputStreamOperator<UserLoginBean> windowStream = withWatermarkStream.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10))
        ).reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>.Context context, Iterable<UserLoginBean> elements, Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean userLoginBean = elements.iterator().next();
                        //补充窗口信息
                        userLoginBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        userLoginBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        userLoginBean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));
                        //写出
                        out.collect(userLoginBean);
                    }
                }
        );

        windowStream.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_LOGIN_WINDOW )
        );


    }
}
