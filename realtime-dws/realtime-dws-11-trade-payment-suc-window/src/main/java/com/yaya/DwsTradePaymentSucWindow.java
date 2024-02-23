package com.yaya;

import com.alibaba.fastjson.JSONObject;
import com.yaya.base.BaseApp;
import com.yaya.bean.TradePaymentBean;
import com.yaya.constant.Constant;
import com.yaya.util.DateFormatUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

@Slf4j
public class DwsTradePaymentSucWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradePaymentSucWindow().start(
                10039,
                4,
                "dws_trade_payment_suc_window",
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //将数据 String => JSON
        SingleOutputStreamOperator<JSONObject> jsondata = stream.map(JSONObject::parseObject);

        //设置水位线
        SingleOutputStreamOperator<JSONObject> watermarksdata = jsondata.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                                   @Override
                                                   public long extractTimestamp(JSONObject jsonObject, long l) {
                                                       return jsonObject.getLong("ts");
                                                   }
                                               }
                        )
        );

        //按照用户id分组
        KeyedStream<JSONObject, String> keyedStream = watermarksdata.keyBy(jsonObject -> jsonObject.getString("user_id"));


        //统计独立支付人数和新增支付人数
        keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {

                    public ValueState<String> statetime;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> vaulestate = new ValueStateDescriptor<>("vaulestate", Types.STRING);

                        statetime = getRuntimeContext().getState(vaulestate);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TradePaymentBean>.Context ctx, Collector<TradePaymentBean> out) throws Exception {
                        //获取状态
                        String value1 = statetime.value();
                        //获取数据时间 转换日期
                        Long ts = value.getLong("ts");
                        String dateTime = DateFormatUtil.tsToDateTime(ts);

                        //判断日期是否为空












                    }
                }
        );


















    }
}
