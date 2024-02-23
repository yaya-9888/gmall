package com.yaya.util;

import com.alibaba.fastjson.JSONObject;
import com.yaya.bean.TableProcessDwd;
import com.yaya.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class FlinkSinkUtil {

    public static KafkaSink<String> getKafkaSink( String topicName ){
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(topicName )
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                )
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix( topicName + "-" + System.currentTimeMillis())
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG , "600000")
                .build();
    }

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink( ){
        return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> element, KafkaSinkContext context, Long timestamp) {
                                String topicName = element.f1.getSinkTable();
                                String data = element.f0.toJSONString();
                                return new ProducerRecord<>( topicName , data.getBytes() ) ;
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(  "dwd_base_db-" + System.currentTimeMillis())
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG , "600000")
                .build();
    }

    public static DorisSink<String> getDorisSink(String tableName ){
        Properties properties = new Properties();
        // When the upstream is writing json, the configuration needs to be enabled.
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        return
                DorisSink.<String>builder()
                        .setDorisReadOptions(DorisReadOptions.builder().build())
                        .setDorisExecutionOptions(
                                DorisExecutionOptions.builder()
                                        .setLabelPrefix("label-doris-" + System.currentTimeMillis()) //streamload label prefix
                                        .setDeletable(false)
                                        .setStreamLoadProp(properties)
                                        .build()
                        )
                        .setSerializer(new SimpleStringSerializer()) //serialize according to string
                        .setDorisOptions(
                                DorisOptions.builder()
                                        .setFenodes(Constant.DORIS_FENODES)
                                        .setTableIdentifier(Constant.DORIS_DATABASE + "." + tableName)
                                        .setUsername(Constant.DORIS_USERNAME)
                                        .setPassword(Constant.DORIS_PASSWORD)
                                        .build()
                        )
                        .build();
    }
}
