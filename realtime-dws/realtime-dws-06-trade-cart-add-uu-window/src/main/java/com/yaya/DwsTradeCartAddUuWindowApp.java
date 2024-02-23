package com.yaya;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yaya.base.BaseApp;
import com.yaya.bean.CartAddUuBean;
import com.yaya.constant.Constant;
import com.yaya.function.DorisMapFunction;
import com.yaya.util.DateFormatUtil;
import com.yaya.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

@Slf4j
public class DwsTradeCartAddUuWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindowApp().start(
                10026,
                4,
                "dws_trade_cart_add_uu_window_app",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //过滤数据
        SingleOutputStreamOperator<JSONObject> jsonStream = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String vaule, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject json = JSONObject.parseObject(vaule);
                            String userId = json.getString("user_id");
                            Long ts = json.getLong("ts");
                            if (userId != null && ts != null) {
                                json.put("ts", ts * 1000);
                                collector.collect(json);
                            }
                        }catch (Exception e){
                            log.warn("过滤脏数据");
                        }

                    }
                }
        );

        //分组
        KeyedStream<JSONObject, String> userId = jsonStream.keyBy(jsonObject -> jsonObject.getString("user_id"));
        
        // 过滤独立用户封装成对象
        SingleOutputStreamOperator<CartAddUuBean> processCartAddUuBean = userId.process(
                new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {

                    public ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastAdd = new ValueStateDescriptor<>("lastAdd", Types.STRING);
                        lastAdd.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        state = getRuntimeContext().getState(lastAdd);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context ctx, Collector<CartAddUuBean> out) throws Exception {
                        //获取状态
                        String statevalue = state.value();
                        //获取数据日期
                        Long ts = value.getLong("ts");
                        String todayDt = DateFormatUtil.tsToDateTime(ts);

                        if (!todayDt.equals(statevalue)) {
                            CartAddUuBean build = CartAddUuBean.builder()
                                    .cartAddUuCt(1L)
                                    .ts(ts)
                                    .build();

                            out.collect(build);
                            state.update(todayDt);
                        }
                    }
                }
        );

        //插入水位线
        SingleOutputStreamOperator<CartAddUuBean> andWatermarks = processCartAddUuBean.assignTimestampsAndWatermarks(
                WatermarkStrategy.<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<CartAddUuBean>() {
                                    @Override
                                    public long extractTimestamp(CartAddUuBean cartAddUuBean, long l) {
                                        return cartAddUuBean.getTs();
                                    }
                                }
                        )
        );

        // 开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> outdata = andWatermarks.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(

                        new ReduceFunction<CartAddUuBean>() {
                            @Override
                            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                                return value1;
                            }
                        }
                        ,
                        new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>.Context context, Iterable<CartAddUuBean> elements, Collector<CartAddUuBean> out) throws Exception {
                                CartAddUuBean cartAddUuBean = elements.iterator().next();

                                //补充窗口信息
                                cartAddUuBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                                cartAddUuBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                                //补充分区信息
                                cartAddUuBean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));

                                //写出数据
                                out.collect(cartAddUuBean);

                            }
                        }
                );

        //将数据转成下划线格式
        SingleOutputStreamOperator<String> mapdata = outdata.map(
                new DorisMapFunction<>()
        );

        //写到doris
        mapdata.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_CART_ADD_UU_WINDOW));


    }
}
