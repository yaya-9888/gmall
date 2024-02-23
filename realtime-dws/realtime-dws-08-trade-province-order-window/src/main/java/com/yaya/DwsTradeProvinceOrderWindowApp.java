package com.yaya;

import com.alibaba.fastjson.JSONObject;
import com.yaya.base.BaseApp;
import com.yaya.bean.TradeProvinceOrderBean;
import com.yaya.constant.Constant;
import com.yaya.function.AsyncMapDimFunction;
import com.yaya.function.DorisMapFunction;
import com.yaya.function.MapDimFunction;
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
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DwsTradeProvinceOrderWindowApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindowApp().start(
                10038,
                4,
                "dws_trade_province_order_window_app",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //1. 清洗过滤，转换成JsonObject格式
        SingleOutputStreamOperator<JSONObject> etldata = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsondata = JSONObject.parseObject(value);
                            String detailId = jsondata.getString("id");
                            String orderId = jsondata.getString("order_id");
                            Long ts = jsondata.getLong("ts");
                            if (detailId != null && orderId != null && ts != null) {
                                jsondata.put("ts", ts * 1000);
                                out.collect(jsondata);
                            }

                        } catch (Exception e) {
                            log.warn("过滤脏数据"+value);
                        }

                    }
                }
        );
        //2. 去重

        SingleOutputStreamOperator<TradeProvinceOrderBean> duplicateStream = etldata.keyBy(jsonObject -> jsonObject.getString("id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, TradeProvinceOrderBean>() {

                            ValueState<BigDecimal> valuestate;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<BigDecimal> valuestate1 = new ValueStateDescriptor<>("valuestate", Types.BIG_DEC);
                                //ttl 时间
                                valuestate1.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30)).build());
                                valuestate = getRuntimeContext().getState(valuestate1);

                            }

                            @Override
                            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TradeProvinceOrderBean>.Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {

                                //获取状态
                                BigDecimal valuestate1 = valuestate.value();

                                valuestate1 = valuestate1 == null ? BigDecimal.ZERO : valuestate1;

                                //本次的splitTotalAmount
                                BigDecimal currSplitTotalAmount = value.getBigDecimal("split_total_amount");

                                //创建一个不可变得set数组
                                Set<String> orderIdSet = Collections.singleton(value.getString("order_id"));

                                //封装JavaBean
                                TradeProvinceOrderBean provinceOrderBean =
                                        TradeProvinceOrderBean.builder()
                                                .orderDetailId(value.getString("id"))
                                                .provinceId(value.getString("province_id"))
                                                .ts(value.getLong("ts"))
                                                .orderAmount(currSplitTotalAmount.subtract(valuestate1))
                                                .orderIdSet(orderIdSet)
                                                .build();
                                //更新状态
                                valuestate.update(currSplitTotalAmount);

                                out.collect(provinceOrderBean);

                            }
                        }
                );
        //3.分配水位线，开窗，聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> windodata = duplicateStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                                            @Override
                                            public long extractTimestamp(TradeProvinceOrderBean tradeProvinceOrderBean, long l) {
                                                return tradeProvinceOrderBean.getTs();
                                            }
                                        }
                                )
                ).keyBy(tradeProvinceOrderBean -> tradeProvinceOrderBean.getProvinceId())
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                HashSet<String> orderIdSet = new HashSet<>();
                                orderIdSet.addAll(value1.getOrderIdSet());
                                orderIdSet.addAll(value2.getOrderIdSet());
                                value1.setOrderIdSet(orderIdSet);
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> elements, Collector<TradeProvinceOrderBean> out) throws Exception {
                                TradeProvinceOrderBean tradeProvinceOrderBean = elements.iterator().next();
                                //计算下单数
                                tradeProvinceOrderBean.setOrderCount((long) tradeProvinceOrderBean.getOrderIdSet().size());

                                //补充窗口信息
                                tradeProvinceOrderBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                                tradeProvinceOrderBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));

                                //补充分区信息
                                tradeProvinceOrderBean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));

                                //写出
                                out.collect(tradeProvinceOrderBean);
                            }
                        }

                );

        //4.维度关联  异步获取
        SingleOutputStreamOperator<TradeProvinceOrderBean> joinDimdata = AsyncDataStream.unorderedWait(windodata, new AsyncMapDimFunction<TradeProvinceOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean bean) {
                        return bean.getProvinceId();
                    }

                    @Override
                    public void addDim(TradeProvinceOrderBean bean, JSONObject dimObj) {
                        bean.setProvinceName(dimObj.getString("name"));
                    }
                }, 120, TimeUnit.SECONDS

        );



        //5. 转换字段格式写出到Doris
            joinDimdata.map(
                    new DorisMapFunction<>()
            )
                    .sinkTo(
                    FlinkSinkUtil.getDorisSink( Constant.DWS_TRADE_PROVINCE_ORDER_WINDOW)
            ) ;


    }
}
