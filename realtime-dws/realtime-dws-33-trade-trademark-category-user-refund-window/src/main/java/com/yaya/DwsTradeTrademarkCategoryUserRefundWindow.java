package com.yaya;

import com.alibaba.fastjson.JSONObject;
import com.yaya.base.BaseApp;
import com.yaya.bean.TradeTrademarkCategoryUserRefundBean;
import com.yaya.constant.Constant;
import com.yaya.function.AsyncMapDimFunction;
import com.yaya.function.DorisMapFunction;
import com.yaya.util.DateFormatUtil;
import com.yaya.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(
                10041,
                4,
                "dws_trade_trademark_vategory_user_refund_window",
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //过滤数据
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> etldatas = stream.flatMap(
                new FlatMapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public void flatMap(String value, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                        try {
                            JSONObject jsondata = JSONObject.parseObject(value);
                            TradeTrademarkCategoryUserRefundBean build = TradeTrademarkCategoryUserRefundBean.builder()
                                    .userId(jsondata.getLong("user_id"))
                                    .skuId(jsondata.getString("sku_id"))
                                    .orderIdSet(new HashSet<>(Collections.singleton(jsondata.getString("order_id"))))
                                    .ts(jsondata.getLong("ts") * 1000)
                                    .build();
                            collector.collect(build);

                        } catch (Exception E) {
                            log.warn("过滤脏数据");
                        }
                    }
                }
        );

        //异步获取维度数据
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> trademarkCategory = AsyncDataStream.unorderedWait(etldatas, new AsyncMapDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getSkuId();
                    }

                    @Override
                    public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dimObj) {
                        bean.setCategory3Id(dimObj.getString("category3_id"));
                        bean.setTrademarkId(dimObj.getString("tm_id"));
                    }
                }, 120, TimeUnit.SECONDS
        );

        //开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> streamOperator = trademarkCategory.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, long l) {
                                        return tradeTrademarkCategoryUserRefundBean.getTs();
                                    }
                                })
                ).keyBy(bean -> bean.getTrademarkId() + bean.getCategory3Id() + bean.getUserId())
                .window(
                        TumblingEventTimeWindows.of(Time.seconds(5))
                ).reduce(
                        new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                                HashSet<String> strings = new HashSet<>();
                                strings.addAll(value1.getOrderIdSet());
                                strings.addAll(value2.getOrderIdSet());
                                value1.setOrderIdSet(strings);
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                            @Override
                            public void process(String s,
                                                Context ctx,
                                                Iterable<TradeTrademarkCategoryUserRefundBean> elements,
                                                Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                                TradeTrademarkCategoryUserRefundBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));

                                bean.setRefundCount((long) bean.getOrderIdSet().size());

                                out.collect(bean);
                            }
                        }
                );


        // 关联维度数据
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmStream = AsyncDataStream.unorderedWait(streamOperator, new AsyncMapDimFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public String getTableName() {
                return "dim_base_trademark";
            }

            @Override
            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                return bean.getTrademarkId();
            }

            @Override
            public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dimObj) {
                bean.setTrademarkName(dimObj.getString("tm_name"));
            }
        }, 120, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream.unorderedWait(tmStream, new AsyncMapDimFunction<TradeTrademarkCategoryUserRefundBean>() {

            @Override
            public String getTableName() {
                return "dim_base_category3";
            }

            @Override
            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                return bean.getCategory3Id();
            }

            @Override
            public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                bean.setCategory3Name(dim.getString("name"));
                bean.setCategory2Id(dim.getString("category2_id"));
            }
            },120,TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c2Stream = AsyncDataStream.unorderedWait(c3Stream, new AsyncMapDimFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                return bean.getCategory2Id();
            }

            @Override
            public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                bean.setCategory2Name(dim.getString("name"));
                bean.setCategory1Id(dim.getString("category1_id"));
            }
        }, 120, TimeUnit.SECONDS);


        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream = AsyncDataStream.unorderedWait(c2Stream, new AsyncMapDimFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public String getTableName() {
                return "dim_base_category1";
            }

            @Override
            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                return bean.getCategory1Id();
            }

            @Override
            public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dimObj) {
                bean.setCategory1Name(dimObj.getString("name"));
            }
        }, 120, TimeUnit.SECONDS);
        resultStream.map(
                new DorisMapFunction<>()
        )
        .sinkTo(
                FlinkSinkUtil.getDorisSink( Constant.DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW)
        ) ;

    }
}
