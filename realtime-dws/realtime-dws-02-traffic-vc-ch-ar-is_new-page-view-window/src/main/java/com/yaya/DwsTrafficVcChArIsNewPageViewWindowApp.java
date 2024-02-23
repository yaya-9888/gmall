package com.yaya;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.yaya.base.BaseApp;
import com.yaya.bean.TrafficPageViewBean;
import com.yaya.constant.Constant;
import com.yaya.util.DateFormatUtil;
import com.yaya.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindowApp().start(
                10022 ,
                3 ,
                "dws_traffic_vc_ch_ar_isNew_page_view_window_app" ,
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //按照mid分组，将每条数据处理成  TrafficPageViewBean ，
        //按照判断的规则， 将 会话数 、 独立访客数 、 页面浏览数 、 访问时长 补全
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewStream = stream.keyBy(
                new KeySelector<String, String>() {
                    @Override

                    public String getKey(String value) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(value);
                        String mid = jsonObj.getJSONObject("common").getString("mid");
                        return mid;
                    }
                }
        ).process(
                new KeyedProcessFunction<String, String, TrafficPageViewBean>() {

                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("firstVisitDateDesc", Types.STRING);

                        //设置状态的生存时间
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build()
                        );

                        firstVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }


                    @Override
                    public void processElement(String value, KeyedProcessFunction<String, String, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(value);
                        JSONObject pageObj = jsonObj.getJSONObject("page");
                        JSONObject commonObj = jsonObj.getJSONObject("common");
                        String lastPageId = pageObj.getString("last_page_id");
                        Long duringTime = pageObj.getLong("during_time");
                        Long ts = jsonObj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);
                        Long uvCt = 0L;
                        // 独立访客数
                        if (firstVisitDateState.value() == null || !firstVisitDateState.value().equals(today)) {
                            // 今天的第一次访问
                            uvCt = 1L;
                            //更新状态
                            firstVisitDateState.update(today);
                        }

                        // 会话数
                        Long svCt = 0L;
                        if (lastPageId == null) {
                            svCt = 1L;
                        }

                        //将数据封装成 TrafficPageViewBean 返回
                        TrafficPageViewBean trafficPageViewBean =
                                TrafficPageViewBean.builder()
                                        .uvCt(uvCt)
                                        .svCt(svCt)
                                        .pvCt(1L)
                                        .durSum(duringTime)
                                        .ar(commonObj.getString("ar"))
                                        .ch(commonObj.getString("ch"))
                                        .vc(commonObj.getString("vc"))
                                        .isNew(commonObj.getString("is_new"))
                                        .ts(ts)
                                        .sid(commonObj.getString("sid"))
                                        .build();
                        out.collect(trafficPageViewBean);
                    }
                }
        );

        //分配时间戳和水位线 , 进行window聚合
        SingleOutputStreamOperator<TrafficPageViewBean> windowStream = trafficPageViewStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        ).keyBy(
                new KeySelector<TrafficPageViewBean, String>() {
                    @Override
                    public String getKey(TrafficPageViewBean value) throws Exception {
                        return value.getCh() + ":" + value.getVc() + ":" + value.getAr() + ":" + value.getIsNew();
                    }
                }
        ).window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        ).reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>.Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean trafficPageViewBean = elements.iterator().next();
                        //补充窗口信息
                        String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        //分区
                        //测试时使用，
                        String curDate = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());


                        trafficPageViewBean.setStt( stt );
                        trafficPageViewBean.setEdt( edt);
                        trafficPageViewBean.setCur_date( curDate );

                        out.collect(trafficPageViewBean);
                    }
                }
        );

        // 写出到Doris表中
        windowStream.map(
                new MapFunction<TrafficPageViewBean, String>() {
                    @Override
                    public String map(TrafficPageViewBean value) throws Exception {
                        SerializeConfig serializeConfig = new SerializeConfig();
                        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
                        return JSONObject.toJSONString( value , serializeConfig  ) ;
                    }
                }
        ).sinkTo(FlinkSinkUtil.getDorisSink( Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW )) ;

    }
}
