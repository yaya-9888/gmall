package com.yaya;

import com.alibaba.fastjson.JSONObject;
import com.yaya.base.BaseApp;
import com.yaya.bean.TradeSkuOrderBean;
import com.yaya.constant.Constant;
import com.yaya.function.AsyncMapDimFunction;
import com.yaya.function.DorisMapFunction;
import com.yaya.function.MapDimFunction;
import com.yaya.util.DateFormatUtil;
import com.yaya.util.FlinkSinkUtil;
import com.yaya.util.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
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
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DwsTradeSkuOrderWindowASyncCacheApp extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowASyncCacheApp().start(
                10029 ,
                4 ,
                "dws_trade_sku_order_window_sync_cache_app",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL

        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1. 过滤清洗， 转换成JsonObject结构
        SingleOutputStreamOperator<JSONObject> etlStream = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String vaule, Collector<JSONObject> collector) throws Exception {
                        try {
                            if (vaule != null) {
                                JSONObject json = JSONObject.parseObject(vaule);
                                String id = json.getString("id");
                                String sku_id = json.getString("sku_id");
                                Long ts = json.getLong("ts");

                                if (id != null && sku_id != null && ts != null) {
                                    //将时间转换成毫米级别
                                    json.put("ts", ts * 1000);
                                    collector.collect(json);
                                }
                            }
                        } catch (Exception e) {
                            log.warn("过滤脏数据");
                        }


                    }
                }
        );

        //根据id分组
        KeyedStream<JSONObject, String> keyedStream = etlStream.keyBy(jsonObject -> jsonObject.getString("id"));

        // +I  o1  100  200  300   null   null
        // -D  空  （过滤已经去除）
        // +I  o1  100  200  300   a1   null
        // 将重复的值使用状态变成0
        SingleOutputStreamOperator<TradeSkuOrderBean> processStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
                    public MapState<String, BigDecimal> mapState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, BigDecimal> mapstate = new MapStateDescriptor<>("mapstate", Types.STRING, Types.BIG_DEC);
                        mapstate.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(20)).build());
                        mapState = getRuntimeContext().getMapState(mapstate);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                        //获状态
                        BigDecimal originalAmount = mapState.get("originalAmount");
                        BigDecimal activityReduceAmount = mapState.get("activityReduceAmount");
                        BigDecimal couponReduceAmount = mapState.get("couponReduceAmount");
                        BigDecimal orderAmount = mapState.get("orderAmount");

                        //将空状态赋值未0
                        originalAmount = originalAmount == null ? BigDecimal.ZERO : originalAmount;
                        activityReduceAmount = activityReduceAmount == null ? BigDecimal.ZERO : activityReduceAmount;
                        couponReduceAmount = couponReduceAmount == null ? BigDecimal.ZERO : couponReduceAmount;
                        orderAmount = orderAmount == null ? BigDecimal.ZERO : orderAmount;

                        //获取数据的各金额
                        BigDecimal currOrderAmount = value.getBigDecimal("order_price").multiply(value.getBigDecimal("sku_num"));
                        BigDecimal currSplitTotalAmount = value.getBigDecimal("split_total_amount");
                        BigDecimal currSplitActivityAmount = value.getBigDecimal("split_activity_amount");
                        BigDecimal currSplitCouponAmount = value.getBigDecimal("split_coupon_amount");


                        //封装成TradeSkuOrderBean  减去状态值  只保留了第一次的 之后都减去了
                        TradeSkuOrderBean tradeSkuOrderBean =
                                TradeSkuOrderBean.builder()
                                        .skuId(value.getString("sku_id"))
                                        .skuName(value.getString("sku_name"))
                                        .ts(value.getLong("ts"))
                                        .orderDetailId(value.getString("id"))
                                        .originalAmount(currSplitTotalAmount.subtract(originalAmount))
                                        .activityReduceAmount(currSplitActivityAmount.subtract(activityReduceAmount))
                                        .couponReduceAmount(currSplitCouponAmount.subtract(couponReduceAmount))
                                        .orderAmount(currOrderAmount.subtract(orderAmount))
                                        .build();

                        //写入将值写入状态
                        mapState.put("originalAmount", currSplitTotalAmount);
                        mapState.put("activityReduceAmount", currSplitActivityAmount);
                        mapState.put("couponReduceAmount", currSplitCouponAmount);
                        mapState.put("orderAmount", currOrderAmount);

                        out.collect(tradeSkuOrderBean);
                    }
                }
        );


        //分配水位线
        SingleOutputStreamOperator<TradeSkuOrderBean> andWatermarks = processStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeSkuOrderBean tradeSkuOrderBean, long l) {
                                        return tradeSkuOrderBean.getTs();
                                    }
                                }
                        )
        );
        // 分组
        KeyedStream<TradeSkuOrderBean, String> skuIdkey = andWatermarks.keyBy(TradeSkuOrderBean::getSkuId);






        //开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reducestream = skuIdkey.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));

                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                                TradeSkuOrderBean tradeSkuOrderBean = elements.iterator().next();
                                //补充窗口信息
                                tradeSkuOrderBean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                                tradeSkuOrderBean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                                //补充分区信息
                                tradeSkuOrderBean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));

                                //写出
                                out.collect(tradeSkuOrderBean);
                            }
                        }


                );



        //4. 维度关联
        //DIM: HBase
        //需要关联的表 级 关联顺序:dim_sku_info dim_spu_info dim_base_trademark dim_base_category3 dim_base_category2 dim_base_category1




        // Hbase获取数据 Redis缓存 DimApp(数据改变删除缓存)  封装模版  异步实现

        SingleOutputStreamOperator<TradeSkuOrderBean> joinDimStream = joinDimAsync(reducestream);


        //5. 写出到Doris
        joinDimStream.map(
                new DorisMapFunction<>()
        ).sinkTo(
                FlinkSinkUtil.getDorisSink( Constant.DWS_TRADE_SKU_ORDER_WINDOW)
        );
    }

    private static SingleOutputStreamOperator<TradeSkuOrderBean> joinDimAsync(SingleOutputStreamOperator<TradeSkuOrderBean> windowAggStream) {

        // 关联 dim_sku_info

        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream = AsyncDataStream.unorderedWait(windowAggStream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_sku_info";
            }

            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getSkuId();
            }

            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setSpuId(dimObj.getString("spu_id"));
                bean.setTrademarkId(dimObj.getString("tm_id"));
                bean.setCategory3Id(dimObj.getString("category3_id"));

            }
        }, 120, TimeUnit.SECONDS);


        // 关联 dim_spu_info
        SingleOutputStreamOperator<TradeSkuOrderBean> spuInfoStream = AsyncDataStream.unorderedWait(skuInfoStream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_spu_info";
            }

            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getSpuId();
            }

            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setSpuName(dimObj.getString("spu_name"));
            }
        }, 120, TimeUnit.SECONDS);

        // 关联 dim_base_trademark
        SingleOutputStreamOperator<TradeSkuOrderBean> baseTrademarkStream = AsyncDataStream.unorderedWait(spuInfoStream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_base_trademark";
            }

            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getTrademarkId();
            }

            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setTrademarkName(dimObj.getString("tm_name"));
            }
        }, 120, TimeUnit.SECONDS);


        // 关联 dim_base_category3
        SingleOutputStreamOperator<TradeSkuOrderBean> baseCategory3Stream = AsyncDataStream.unorderedWait(baseTrademarkStream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_base_category3";
            }

            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory3Id();
            }

            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setCategory3Name(dimObj.getString("name"));
                bean.setCategory2Id(dimObj.getString("category2_id"));
            }
        }, 120, TimeUnit.SECONDS);




        //关联 dim_base_category2

        SingleOutputStreamOperator<TradeSkuOrderBean> baseCategory2Stream = AsyncDataStream.unorderedWait(baseCategory3Stream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory2Id();
            }

            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setCategory2Name(dimObj.getString("name"));
                bean.setCategory1Id(dimObj.getString("category1_id"));
            }


        }, 120, TimeUnit.SECONDS);



        //关联 dim_base_category1
        SingleOutputStreamOperator<TradeSkuOrderBean> baseCategory1Stream = AsyncDataStream.unorderedWait(baseCategory2Stream, new AsyncMapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getTableName() {
                return "dim_base_category1";
            }

            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory1Id();
            }

            @Override
            public void addDim(TradeSkuOrderBean bean, JSONObject dimObj) {
                bean.setCategory1Name(dimObj.getString("name"));
            }
        }, 120, TimeUnit.SECONDS);

        return baseCategory1Stream ;

    }
}
