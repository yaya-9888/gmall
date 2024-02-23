package com.yaya;

import com.alibaba.fastjson.JSONObject;
import com.yaya.base.BaseApp;
import com.yaya.bean.TrafficHomeDetailPageViewBean;
import com.yaya.constant.Constant;
import com.yaya.function.DorisMapFunction;
import com.yaya.util.DateFormatUtil;
import com.yaya.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

@Slf4j
public class DwsTrafficHomeDetailPageViewWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindowApp().start(10023,4,"dws_traffic_home_detail_page_view_window_app", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //过滤清洗 转换成json格式
        SingleOutputStreamOperator<JSONObject> jsonObjectdata = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            String page_id = jsonObject.getJSONObject("page").getString("page_id");
                            String mid = jsonObject.getJSONObject("common").getString("mid");
                            Long ts = jsonObject.getLong("ts");
                            //将符合条件的输出
                            if (("good_detail".equals(page_id) || "home".equals(page_id)) && mid != null && ts != null) {
                                collector.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            log.warn("过滤掉脏数据: " + value);
                        }
                    }
                }
        );
        //2. 按照mid分组， 判断是否是首页 ，详情页独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> keyprocess = jsonObjectdata.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .process(
                        new ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

                            public ValueState<String> homeState;
                            public ValueState<String> goodDetailState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> homeStateDesc = new ValueStateDescriptor<>("homeStateDesc", Types.STRING);
                                homeState = getRuntimeContext().getState(homeStateDesc);
                                homeStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                ValueStateDescriptor<String> goodDetailStates = new ValueStateDescriptor<>("goodDetailState", Types.STRING);
                                goodDetailStates.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                goodDetailState = getRuntimeContext().getState(goodDetailStates);
                            }

                            @Override
                            public void processElement(JSONObject value, ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                                //获取pege_id
                                String pageId = value.getJSONObject("page").getString("page_id");

                                //获取状态
                                String home = homeState.value();
                                String good = goodDetailState.value();

                                //获取数据时间ts
                                Long ts = value.getLong("ts");
                                String todayDt = DateFormatUtil.tsToDate(ts);


                                Long homeUvCt = 0L;
                                Long goodDetailUvCt = 0L;

                                //判断是否是home的独立访客
                                if ("home".equals(pageId) && !todayDt.equals(homeState)) {
                                    homeUvCt = 1L;
                                    //更新状态
                                    homeState.update(todayDt);
                                }

                                //判断是否是goodDetail的独立访客
                                if ("good_detail".equals(pageId) && !todayDt.equals(goodDetailState)) {
                                    goodDetailUvCt = 1L;
                                    //更新状态
                                    goodDetailState.update(todayDt);
                                }

                                // 如果是home 或者 good_detail的独立访客，将数据写出
                                if (homeUvCt + goodDetailUvCt == 1) {
                                    //使用 @Builder 赋值
                                    TrafficHomeDetailPageViewBean homeDetailPageViewBean =
                                            TrafficHomeDetailPageViewBean.builder()
                                                    .ts(ts)
                                                    .homeUvCt(homeUvCt)
                                                    .goodDetailUvCt(goodDetailUvCt)
                                                    .build();

                                    out.collect(homeDetailPageViewBean);
                                }

                            }
                        }
                );

        //获取水位线
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream = keyprocess.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean, long l) {
                                        return trafficHomeDetailPageViewBean.getTs();
                                    }
                                }
                        )
        );

        //4. 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> streamOperator = withWaterMarkStream.windowAll(
                        TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
                )
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                                return value1;
                            }
                        }
                        ,
                        new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                                TrafficHomeDetailPageViewBean homeDetailPageViewBean = elements.iterator().next();
                                //窗口信息
                                String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                //分区信息
                                String currDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());

                                //补充到Bean对象中
                                homeDetailPageViewBean.setStt(stt);
                                homeDetailPageViewBean.setEdt(edt);
                                homeDetailPageViewBean.setCurDate(currDt);

                                //写出
                                out.collect(homeDetailPageViewBean);
                            }
                        }
                );
        //5 写入doris
        streamOperator.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink( Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW )
        ) ;




    }
}
